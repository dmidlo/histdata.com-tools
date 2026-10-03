"""Synthetic durable composition distinguishes stored groups from sources."""

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.data_quality import training_wide_artifacts as artifacts
from histdatacom.data_quality.training_join_contracts import JoinInformationMode
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
    materialize_training_wide_view,
)
from histdatacom.datasets import DatasetCatalog
from tests.fixtures.training_join_v1 import join_fixture


@pytest.fixture(scope="module")
def view(tmp_path_factory):
    native = join_fixture(tmp_path_factory.mktemp("wide-artifact-source"))
    other = replace(native.columns[0], name="market.tick.EURUSD.other")
    plans = (native, replace(native, columns=(other,)))
    return materialize_training_wide_view(
        build_training_wide_plan(
            plans, grain=TrainingWideGrainV1(WideRowGrain.EVENT)
        )
    )


def read(path, columns=None):
    return artifacts.read_training_wide_artifact(
        path, information_mode=JoinInformationMode.EX_POST, columns=columns
    )


def test_native_children_and_value_free_layout_are_actual_canonical_files(
    tmp_path, view
):
    path = artifacts.write_training_wide_artifact(view, tmp_path)
    assert path.read_bytes() == view.manifest.to_json().encode()
    assert artifacts.write_training_wide_artifact(view, tmp_path) == path
    assert len(tuple(tmp_path.iterdir())) == 4  # control, two groups, layout
    assert all(
        token not in path.read_text()
        for token in (
            '"value_json"',
            '"evidence_json"',
            '"join_plan_json"',
            '"native_row_json"',
        )
    )
    result = read(path)
    assert len(result.records) == 3
    assert result.manifest == view.manifest
    assert result.storage_report()["all_value_files_verified"]
    assert result.layout_bytes + result.verified_native_bytes == sum(
        p.stat().st_size for p in tmp_path.iterdir()
    )
    assert result.storage_report()[
        "verified_bytes_per_output_row"
    ] == pytest.approx(sum(p.stat().st_size for p in tmp_path.iterdir()) / 3)


@pytest.mark.parametrize("damage", ("missing", "corrupt"))
def test_projected_groups_skip_unselected_storage_but_full_read_refuses(
    tmp_path, view, monkeypatch, damage
):
    path = artifacts.write_training_wide_artifact(view, tmp_path)
    unselected = tmp_path / view.manifest.groups[-1].child.filename
    if damage == "missing":
        unselected.unlink()
    else:
        unselected.write_bytes(b"retained corrupt fixture")
    opened = []
    original = artifacts.read_training_join_artifact

    def tracking(path, **kwargs):
        opened.append(Path(path).name)
        return original(path, **kwargs)

    monkeypatch.setattr(artifacts, "read_training_join_artifact", tracking)
    selected = view.manifest.groups[0].columns
    result = read(path, selected)
    assert len(result.records) == 3
    assert unselected.name not in opened
    assert result.verified_value_files == (
        view.manifest.groups[0].child.filename,
    )
    assert not result.storage_report()["all_value_files_verified"]
    assert result.storage_report()["all_declared_sources_replayed"]
    with pytest.raises((ValueError, FileNotFoundError)):
        read(path)


def test_empty_projection_still_reads_every_control_and_no_value_group(
    tmp_path, view, monkeypatch
):
    path = artifacts.write_training_wide_artifact(view, tmp_path)
    for group in view.manifest.groups:
        (tmp_path / group.child.filename).unlink()
    result = read(path, ())
    assert len(result.records) == 3 and all(
        r["values"] == {} for r in result.records
    )
    assert result.verified_value_files == ()
    assert result.verified_control_files == tuple(
        c.child.filename for c in view.manifest.controls
    )
    (tmp_path / view.manifest.controls[0].child.filename).unlink()
    with pytest.raises(FileNotFoundError):
        read(path, ())


def test_source_corruption_refuses_even_empty_projection(tmp_path):
    native = join_fixture(tmp_path / "source")
    view = materialize_training_wide_view(
        build_training_wide_plan(
            (native,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
        )
    )
    path = artifacts.write_training_wide_artifact(view, tmp_path / "out")
    catalog = DatasetCatalog.from_json(native.spine.source.catalog_json)
    partition = catalog.versions[0].partitions[0]
    Path(partition.artifact.path).write_bytes(b"corrupted synthetic source")
    with pytest.raises(ValueError):
        read(path, ())


@pytest.mark.parametrize("forgery", ("content", "row", "schema"))
def test_resealed_layout_metadata_cannot_become_alternate_authority(
    tmp_path, view, forgery
):
    artifacts.write_training_wide_artifact(view, tmp_path)
    if forgery == "content":
        manifest = replace(view.manifest, content_sha256="f" * 64)
    elif forgery == "row":
        manifest = replace(view.manifest, rows=view.manifest.rows[:-1])
    else:
        manifest = replace(view.manifest, plan_id="forged-plan")
    data = manifest.to_json().encode()
    path = (
        tmp_path
        / f"training-wide-manifest-{hashlib.sha256(data).hexdigest()}.json"
    )
    path.write_bytes(data)
    with pytest.raises(ValueError, match="differs|different"):
        read(path)
    assert path.read_bytes() == data


def test_layout_symlink_and_wrong_information_mode_refuse(tmp_path, view):
    path = artifacts.write_training_wide_artifact(view, tmp_path / "real")
    linked = tmp_path / path.name
    linked.symlink_to(path)
    with pytest.raises(ValueError, match="regular file"):
        read(linked)
    with pytest.raises(TypeError, match="explicit information mode"):
        artifacts.read_training_wide_artifact(path, information_mode="ex_post")
    with pytest.raises(ValueError, match="different mode"):
        artifacts.read_training_wide_artifact(
            path, information_mode=JoinInformationMode.NORMALIZED_AS_OF
        )


@pytest.mark.parametrize("same", (True, False))
def test_atomic_competing_layout_is_not_overwritten(
    tmp_path, view, monkeypatch, same
):
    original = artifacts.os.link
    competitor = (
        view.manifest.to_json().encode()
        if same
        else b"independent retained competitor"
    )

    def race(source, destination, **kwargs):
        if Path(destination).name.startswith("training-wide-manifest-"):
            Path(destination).write_bytes(competitor)
            raise FileExistsError("synthetic layout writer race")
        return original(source, destination, **kwargs)

    monkeypatch.setattr(artifacts.os, "link", race)
    if same:
        assert (
            read(
                artifacts.write_training_wide_artifact(view, tmp_path)
            ).manifest
            == view.manifest
        )
    else:
        with pytest.raises(ValueError, match="conflicts"):
            artifacts.write_training_wide_artifact(view, tmp_path)
    layouts = tuple(tmp_path.glob("training-wide-manifest-*"))
    assert len(layouts) == 1 and layouts[0].read_bytes() == competitor
    assert not tuple(tmp_path.glob(".wide-*"))


def test_failure_between_children_and_layout_leaves_children_not_false_completion(
    tmp_path, view, monkeypatch
):
    original = artifacts.os.link

    def interrupted(source, destination, **kwargs):
        if Path(destination).name.startswith("training-wide-manifest-"):
            raise OSError("synthetic interruption")
        return original(source, destination, **kwargs)

    monkeypatch.setattr(artifacts.os, "link", interrupted)
    with pytest.raises(OSError, match="synthetic interruption"):
        artifacts.write_training_wide_artifact(view, tmp_path)
    assert len(tuple(tmp_path.glob("training-join-batch-*"))) == 3
    assert not tuple(tmp_path.glob("training-wide-manifest-*"))
    assert not tuple(tmp_path.glob(".wide-*"))


def test_false_small_child_budget_refuses_before_native_parse(
    tmp_path, view, monkeypatch
):
    artifacts.write_training_wide_artifact(view, tmp_path)
    control = replace(
        view.manifest.controls[0],
        child=replace(view.manifest.controls[0].child, byte_count=1),
    )
    manifest = replace(view.manifest, controls=(control,))
    data = manifest.to_json().encode()
    path = (
        tmp_path
        / f"training-wide-manifest-{hashlib.sha256(data).hexdigest()}.json"
    )
    path.write_bytes(data)

    def forbidden(*args, **kwargs):
        pytest.fail("oversized native child reached parsing/source replay")

    monkeypatch.setattr(artifacts, "read_training_join_artifact", forbidden)
    with pytest.raises(ValueError, match="regular file"):
        read(path)


def test_child_swap_after_preflight_cannot_bypass_native_identity(
    tmp_path, view, monkeypatch
):
    path = artifacts.write_training_wide_artifact(view, tmp_path)
    original = artifacts.read_training_join_artifact

    def replaced(candidate, **kwargs):
        actual = original(candidate, **kwargs)
        if Path(candidate).name == view.manifest.controls[0].child.filename:
            return view.groups[0]
        return actual

    monkeypatch.setattr(artifacts, "read_training_join_artifact", replaced)
    with pytest.raises(ValueError, match="identity changed"):
        read(path, ())


def test_new_container_requires_native_receipts_even_for_legacy_children(
    tmp_path, view, monkeypatch
):
    path = artifacts.write_training_wide_artifact(view, tmp_path)
    original = artifacts.verify_training_policy_receipt
    checked = []

    def verify(subject, target, *, required=False):
        assert required is True
        checked.append(Path(target).name)
        return original(subject, target, required=required)

    monkeypatch.setattr(artifacts, "verify_training_policy_receipt", verify)
    read(path)
    assert checked == [c.child.filename for c in view.manifest.controls] + [
        g.child.filename for g in view.manifest.groups
    ]

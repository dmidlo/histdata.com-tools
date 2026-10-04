"""CLI admission controls and separately labelled actual generated lifecycles.

Forwarding doubles are not native replay proof. Tests prefixed ``test_actual``
use real store/native operations on three invented ticks; published catalogs
remain explicitly unqualified and are not empirical release evidence.
"""

import json
import os

import pytest

from histdatacom import cleanup_cli
from histdatacom.artifact_retention import cli
from histdatacom.artifact_retention import contracts as c
from histdatacom.artifact_retention.canonical import MAX_WIRE_BYTES
from histdatacom.cli_config import configured_cleanup_argv


def _snapshot(*, blockers=()):
    return c.StoreSnapshotV1(
        "a" * 32,
        c.RetentionPolicyV1(),
        "b" * 64,
        "c" * 64,
        (),
        (),
        (),
        (),
        (),
        (),
        (),
        blockers,
    )


def _plan(*, blockers=()):
    snapshot = _snapshot()
    return c.CollectionPlanV1(
        snapshot.store_id,
        snapshot.artifact_id,
        snapshot.policy.artifact_id,
        1,
        (),
        blockers,
    )


def _receipt(*, status="complete"):
    return c.CollectionReceiptV1(
        "a" * 32,
        _plan().artifact_id,
        1,
        2,
        status,
        (),
        "verified" if status == "complete" else "not_attempted",
    )


@pytest.fixture
def calls(monkeypatch):
    """Explicit forwarding doubles, never assertions about native authority."""
    result = []

    def inspect(root):
        result.append(("inspect", root))
        return _snapshot()

    def plan(root, cutoff):
        result.append(("plan", root, cutoff))
        return _plan()

    def apply(root, plan):
        result.append(("apply", root, plan))
        return _receipt()

    monkeypatch.setattr(cli, "_inspect", inspect)
    monkeypatch.setattr(cli, "_plan", plan)
    monkeypatch.setattr(cli, "_apply", apply)
    return result


def _plan_file(tmp_path):
    path = tmp_path / "plan.json"
    path.write_text(_plan().to_json(), encoding="ascii")
    return path


def _config(tmp_path, text):
    path = tmp_path / "config.yaml"
    path.write_text("cleanup:\n" + text, encoding="utf-8")
    return ["--config", str(path)]


@pytest.mark.parametrize("operation", ("inspect", "plan", "apply"))
@pytest.mark.parametrize("json_position", ("before", "after", "absent"))
def test_explicit_routing_exact_canonical_stdout(
    tmp_path, capsys, calls, operation, json_position
):
    args = [f"artifacts-{operation}", "--store", str(tmp_path)]
    if operation == "apply":
        args.extend(("--plan", str(_plan_file(tmp_path))))
    if json_position == "before":
        args.insert(0, "--json")
    elif json_position == "after":
        args.append("--json")
    assert cleanup_cli.main(args) == 0
    expected = {"inspect": _snapshot(), "plan": _plan(), "apply": _receipt()}
    assert capsys.readouterr().out == expected[operation].to_json()
    assert calls[0][0:2] == (operation, tmp_path)
    if operation == "apply":
        assert calls[0][2] == _plan()


@pytest.mark.parametrize(
    "args",
    [
        ["artifacts-inspect"],
        ["artifacts-apply", "--store", "."],
        ["artifacts-inspect", "--store", ".", "--plan", "x"],
        ["artifacts-plan", "--store", ".", "--plan", "x"],
        ["artifacts-inspect", "--store", ".", "--cutoff-ns", "1"],
        ["artifacts-apply", "--store", ".", "--plan", "x", "--cutoff-ns", "1"],
        ["artifacts-inspect", "--store", ".", "--apply"],
        ["artifacts-plan", "--store", ".", "--data-directory", "x"],
        ["artifacts-inspect", "--store", ".", "--pairs", "EURUSD"],
        ["artifacts-apply", "--store", ".", "--plan", "x", "--force"],
        ["artifacts-inspect", "--sto", "."],
        ["sources", "--store", "."],
        ["status", "--output", "x"],
        ["--plan", "x"],
    ],
)
def test_cross_command_flags_refuse_before_operation(args, calls):
    with pytest.raises(SystemExit) as error:
        cleanup_cli.main(args)
    assert error.value.code == 2
    assert not calls


@pytest.mark.parametrize(
    "cutoff", ("0", "-1", "1.0", "true", "9" * 20, "9223372036854775807")
)
def test_cutoff_refuses_before_operation(tmp_path, cutoff, calls, capsys):
    assert (
        cleanup_cli.main(
            [
                "artifacts-plan",
                "--store",
                str(tmp_path),
                "--cutoff-ns",
                cutoff,
            ]
        )
        == 1
    )
    assert not calls
    assert not capsys.readouterr().out


def test_cutoff_forwarded_without_substitution(tmp_path, calls, capsys):
    assert (
        cleanup_cli.main(
            [
                "artifacts-plan",
                "--store",
                str(tmp_path),
                "--cutoff-ns",
                "123",
            ]
        )
        == 0
    )
    assert calls == [("plan", tmp_path, 123)]
    assert capsys.readouterr().out == _plan().to_json()


@pytest.mark.parametrize("operation", ("inspect", "plan", "apply"))
def test_scoped_config_routes_only_declared_operation(
    tmp_path, capsys, calls, operation
):
    config = (
        f"  command: artifacts-{operation}\n  store: {tmp_path}\n  json: true\n"
    )
    if operation == "apply":
        config += f"  plan: {_plan_file(tmp_path)}\n"
    assert cleanup_cli.main(_config(tmp_path, config)) == 0
    assert calls[0][0] == operation
    assert json.loads(capsys.readouterr().out)["schema_version"]


@pytest.mark.parametrize(
    "key",
    (
        "apply: true",
        "apply: false",
        "data_directory: old",
        "pairs: [EURUSD]",
        "plan: old",
        "cutoff_ns: 1",
    ),
)
def test_unscoped_legacy_or_cross_command_config_cannot_flow_to_artifact(
    tmp_path, calls, key
):
    args = _config(tmp_path, f"  {key}\n")
    assert (
        cleanup_cli.main([*args, "artifacts-inspect", "--store", str(tmp_path)])
        == 1
    )
    assert not calls


@pytest.mark.parametrize(
    "key", ("apply: true", "cutoff_ns: 1", "data_directory: old", "plan: old")
)
def test_explicit_artifact_config_rejects_cross_command_keys(
    tmp_path, calls, key
):
    args = _config(
        tmp_path,
        f"  command: artifacts-inspect\n  store: {tmp_path}\n  {key}\n",
    )
    assert cleanup_cli.main(args) == 1
    assert not calls


def test_explicit_command_overrides_other_configured_command(
    tmp_path, calls, capsys
):
    args = _config(
        tmp_path,
        "  command: sources\n  apply: true\n  data_directory: missing\n",
    )
    assert (
        cleanup_cli.main([*args, "artifacts-inspect", "--store", str(tmp_path)])
        == 0
    )
    assert calls == [("inspect", tmp_path)]
    capsys.readouterr()
    args = _config(
        tmp_path,
        "  command: artifacts-apply\n  store: missing\n  plan: missing\n",
    )
    assert configured_cleanup_argv([*args, "sources"]) == ["sources"]


@pytest.mark.parametrize(
    "flag",
    (
        "--data-directory",
        "--workspace",
        "--runtime-home",
        "--state-dir",
        "--pairs",
        "-p",
        "--pair-groups",
        "--formats",
        "-f",
        "--timeframes",
        "-t",
    ),
)
def test_option_values_never_select_artifact_apply(tmp_path, flag):
    args = _config(tmp_path, "  command: status\n  json: true\n")
    expanded = configured_cleanup_argv([*args, flag, "artifacts-apply"])
    assert (
        cleanup_cli.build_parser().parse_args(expanded).cleanup_command
        == "status"
    )


def test_artifact_output_filename_is_not_a_command(
    tmp_path, monkeypatch, calls, capsys
):
    monkeypatch.chdir(tmp_path)
    args = _config(
        tmp_path, f"  command: artifacts-plan\n  store: {tmp_path}\n"
    )
    assert cleanup_cli.main([*args, "--output", "artifacts-apply"]) == 0
    assert calls == [("plan", tmp_path, None)]
    assert (tmp_path / "artifacts-apply").read_text() == _plan().to_json()
    assert capsys.readouterr().out == _plan().to_json()


@pytest.mark.parametrize(
    "kind", ("newline", "pretty", "unknown", "duplicate", "foreign", "nonascii")
)
def test_plan_is_exact_canonical_not_normalized(tmp_path, calls, kind):
    path = _plan_file(tmp_path)
    raw = path.read_text()
    variants = {
        "newline": raw + "\n",
        "pretty": json.dumps(json.loads(raw), indent=2),
        "unknown": raw[:-1] + ',"other":1}',
        "duplicate": raw[:-1] + ',"store_id":"' + "a" * 32 + '"}',
        "foreign": _snapshot().to_json(),
        "nonascii": raw + "é",
    }
    path.write_text(variants[kind])
    assert (
        cleanup_cli.main(
            ["artifacts-apply", "--store", str(tmp_path), "--plan", str(path)]
        )
        == 1
    )
    assert not calls


@pytest.mark.parametrize(
    "kind",
    (
        "symlink",
        "ancestor_symlink",
        "fifo",
        "directory",
        "hardlink",
        "oversize",
    ),
)
def test_plan_file_boundary_refuses_before_operation(tmp_path, calls, kind):
    path = tmp_path / "bad.json"
    good = _plan_file(tmp_path)
    if kind == "symlink":
        path.symlink_to(good)
    elif kind == "ancestor_symlink":
        path.symlink_to(tmp_path, target_is_directory=True)
        path = path / good.name
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "directory":
        path.mkdir()
    elif kind == "hardlink":
        os.link(good, path)
    else:
        with path.open("wb") as stream:
            stream.truncate(MAX_WIRE_BYTES + 1)
    assert (
        cleanup_cli.main(
            ["artifacts-apply", "--store", str(tmp_path), "--plan", str(path)]
        )
        == 1
    )
    assert not calls


@pytest.mark.parametrize(
    "kind",
    (
        "exists",
        "symlink",
        "dangling",
        "ancestor_symlink",
        "managed",
        "missing_parent",
    ),
)
def test_output_preflight_refuses_before_any_operation(tmp_path, calls, kind):
    output = tmp_path / "output.json"
    if kind == "exists":
        output.write_text("original")
    elif kind in {"symlink", "dangling"}:
        target = tmp_path / "target"
        if kind == "symlink":
            target.write_text("original")
        output.symlink_to(target)
    elif kind == "ancestor_symlink":
        output.symlink_to(tmp_path, target_is_directory=True)
        output = output / "new.json"
    elif kind == "managed":
        (tmp_path / ".histdatacom-retention.json").write_text("not even valid")
    else:
        output = tmp_path / "missing" / "output.json"
    assert (
        cleanup_cli.main(
            [
                "artifacts-plan",
                "--store",
                str(tmp_path),
                "--output",
                str(output),
            ]
        )
        == 1
    )
    assert not calls


def test_canonical_output_is_direct_apply_input(tmp_path, calls, capsys):
    output = tmp_path / "new-plan.json"
    assert (
        cleanup_cli.main(
            [
                "artifacts-plan",
                "--store",
                str(tmp_path),
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert (
        output.read_bytes()
        == capsys.readouterr().out.encode("ascii")
        == _plan().to_json().encode("ascii")
    )
    assert (
        cleanup_cli.main(
            ["artifacts-apply", "--store", str(tmp_path), "--plan", str(output)]
        )
        == 0
    )
    assert calls[-1] == ("apply", tmp_path, _plan())


def test_output_race_preserves_actual_receipt_and_nonzero(
    tmp_path, monkeypatch, capsys
):
    output = tmp_path / "receipt.json"

    def apply(root, plan):
        output.write_text("racing writer")
        return _receipt(status="partial")

    monkeypatch.setattr(cli, "_apply", apply)
    assert (
        cleanup_cli.main(
            [
                "artifacts-apply",
                "--store",
                str(tmp_path),
                "--plan",
                str(_plan_file(tmp_path)),
                "--output",
                str(output),
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert captured.out == _receipt(status="partial").to_json()
    assert (
        "operation returned the following receipt, but report export failed"
        in captured.err
    )
    assert output.read_text() == "racing writer"


@pytest.mark.parametrize("status", ("complete", "partial", "indeterminate"))
def test_receipt_status_controls_exit_without_relabeling(
    tmp_path, monkeypatch, capsys, status
):
    monkeypatch.setattr(
        cli, "_apply", lambda root, plan: _receipt(status=status)
    )
    result = cleanup_cli.main(
        [
            "artifacts-apply",
            "--store",
            str(tmp_path),
            "--plan",
            str(_plan_file(tmp_path)),
        ]
    )
    assert result == int(status != "complete")
    assert capsys.readouterr().out == _receipt(status=status).to_json()


@pytest.mark.parametrize("operation", ("inspect", "plan"))
def test_blockers_are_returned_and_exit_nonzero(
    tmp_path, monkeypatch, capsys, operation
):
    result = (
        _snapshot(blockers=("blocked",))
        if operation == "inspect"
        else _plan(blockers=("blocked",))
    )
    monkeypatch.setattr(cli, "_" + operation, lambda *args: result)
    assert (
        cleanup_cli.main(["artifacts-" + operation, "--store", str(tmp_path)])
        == 1
    )
    assert capsys.readouterr().out == result.to_json()


def test_diagnostic_prints_do_not_pollute_stdout(tmp_path, monkeypatch, capsys):
    def inspect(root):
        print("native diagnostic")
        return _snapshot()

    monkeypatch.setattr(cli, "_inspect", inspect)
    assert (
        cleanup_cli.main(["artifacts-inspect", "--store", str(tmp_path)]) == 0
    )
    captured = capsys.readouterr()
    assert captured.out == _snapshot().to_json()
    assert "native diagnostic" in captured.err


def test_interruption_does_not_manufacture_normal_receipt(
    tmp_path, monkeypatch, capsys
):
    actual_error = '{"interrupted":true,"observed_time_ns":null}'

    def interrupted(root, plan):
        raise cli._Interrupted(actual_error)

    monkeypatch.setattr(cli, "_apply", interrupted)
    assert (
        cleanup_cli.main(
            [
                "artifacts-apply",
                "--store",
                str(tmp_path),
                "--plan",
                str(_plan_file(tmp_path)),
            ]
        )
        == 1
    )
    assert capsys.readouterr().out == actual_error


def test_invalid_result_cannot_be_rendered_as_success(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr(cli, "_inspect", lambda root: {"passed": True})
    assert (
        cleanup_cli.main(["artifacts-inspect", "--store", str(tmp_path)]) == 1
    )
    assert not capsys.readouterr().out


def test_mutated_nested_result_refuses_before_serialization(
    tmp_path, monkeypatch, capsys
):
    actual = _snapshot()
    object.__setattr__(actual, "policy", {"scratch_ttl_ns": 0})
    monkeypatch.setattr(cli, "_inspect", lambda root: actual)
    assert (
        cleanup_cli.main(["artifacts-inspect", "--store", str(tmp_path)]) == 1
    )
    assert not capsys.readouterr().out


_GENERATED_ASCII = (
    b"20200102 000000000,1.100000,1.100200,0\n"
    b"20200102 000001000,1.100100,1.100300,1\n"
    b"20200102 000002000,1.100200,1.100400,2\n"
)


def _actual_store(tmp_path, *, cache_count=0, publish_catalog=False):
    """Use public native producers, not a caller-built descriptor graph."""
    from histdatacom.artifact_retention import storage

    root = (tmp_path / "actual store").resolve()
    # Zero is an admitted, explicit fixture TTL, not an overridden clock or
    # forged old creation time. Only genuinely completed scratch may mature.
    marker = storage.create_retention_store(
        root, c.RetentionPolicyV1(scratch_ttl_ns=0)
    )
    source_path = tmp_path / "generated.csv"
    source_path.write_bytes(_GENERATED_ASCII)
    source = None
    caches = []
    catalog = None
    if cache_count:
        source = storage.ingest_ascii_tick_source(
            root, source_path, symbol="EURUSD", period="202001"
        )
        for _ in range(cache_count):
            caches.append(
                storage.produce_histdata_cache(root, source.primary_object_id)
            )
        if publish_catalog:
            catalog = storage.publish_histdata_cache_catalog(
                root, (caches[0].primary_object_id,), dataset_id="generated-cli"
            )
    for index, record in enumerate((marker, source, *caches, catalog)):
        if record is not None:
            (tmp_path / f"actual-admission-{index}.json").write_text(
                record.to_json(), encoding="ascii"
            )
    return root, source_path, source, caches, catalog


def _actual_cli(tmp_path, capsys, name, args):
    """Retain actual stdout/stderr/status before any acceptance assertion."""
    capsys.readouterr()
    code = cleanup_cli.main(args)
    captured = capsys.readouterr()
    (tmp_path / f"{name}.stdout.json").write_text(
        captured.out, encoding="utf-8"
    )
    (tmp_path / f"{name}.stderr.log").write_text(captured.err, encoding="utf-8")
    (tmp_path / f"{name}.status.json").write_text(
        json.dumps({"actual_return_code": code}), encoding="ascii"
    )
    return code, captured


def test_actual_empty_store_inspect_export_plan_and_apply(tmp_path, capsys):
    root, source_path, _, _, _ = _actual_store(tmp_path)
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "empty-inspect",
        ["artifacts-inspect", "--store", str(root)],
    )
    assert code == 0
    snapshot = c.StoreSnapshotV1.from_json(captured.out)
    assert snapshot.descriptors == snapshot.blockers == ()
    plan_path = tmp_path / "exported-plan.json"
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "empty-plan",
        ["artifacts-plan", "--store", str(root), "--output", str(plan_path)],
    )
    assert code == 0
    plan = c.CollectionPlanV1.from_json(captured.out)
    assert plan.decisions == plan.blockers == ()
    assert plan_path.read_bytes() == captured.out.encode("ascii")
    receipt_path = tmp_path / "exported-receipt.json"
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "empty-apply",
        [
            "artifacts-apply",
            "--store",
            str(root),
            "--plan",
            str(plan_path),
            "--output",
            str(receipt_path),
        ],
    )
    assert code == 0
    receipt = c.CollectionReceiptV1.from_json(captured.out)
    assert receipt.plan_id == plan.artifact_id
    assert receipt.status == "complete"
    assert receipt.outcomes == ()
    assert receipt.protected_replay == "verified"
    assert receipt_path.read_bytes() == captured.out.encode("ascii")
    assert source_path.read_bytes() == _GENERATED_ASCII


def test_actual_orphan_and_mature_scratch_collection_preserves_native_catalog(
    tmp_path, capsys
):
    root, source_path, source, caches, catalog = _actual_store(
        tmp_path, cache_count=2, publish_catalog=True
    )
    assert source is not None and catalog is not None
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "before-inspect",
        ["artifacts-inspect", "--store", str(root)],
    )
    assert code == 0
    before = c.StoreSnapshotV1.from_json(captured.out)
    assert not before.blockers
    objects = {item.artifact_id: item for item in before.descriptors}
    protected = {
        source.primary_object_id,
        caches[0].primary_object_id,
        catalog.primary_object_id,
    }
    exact_before = {
        identity: (
            root / objects[identity].payload_ref.relative_path
        ).read_bytes()
        for identity in protected
    }
    # Physical publication remains explicitly UNQUALIFIED; retention safety
    # cannot turn this invented observed source into released campaign proof.
    native_catalog = json.loads(exact_before[catalog.primary_object_id])
    assert (
        native_catalog["versions"][0]["qualification_status"] == "unqualified"
    )
    scratch = {
        identity
        for identity, item in objects.items()
        if item.retention_class is c.RetentionClass.SCRATCH
    }
    assert len(scratch) == 3
    expected_deletes = scratch | {caches[1].primary_object_id}
    plan_path = tmp_path / "collection-plan.json"
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "collection-plan",
        ["artifacts-plan", "--store", str(root), "--output", str(plan_path)],
    )
    assert code == 0
    plan = c.CollectionPlanV1.from_json(captured.out)
    assert not plan.blockers
    assert {
        item.object_id for item in plan.decisions if item.action == "delete"
    } == expected_deletes
    assert all(
        item.action == "keep"
        for item in plan.decisions
        if item.object_id in protected
    )
    assert plan_path.read_bytes() == captured.out.encode("ascii")
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "collection-apply",
        ["artifacts-apply", "--store", str(root), "--plan", str(plan_path)],
    )
    assert code == 0
    receipt = c.CollectionReceiptV1.from_json(captured.out)
    assert receipt.status == "complete"
    assert receipt.protected_replay == "verified"
    assert {item.object_id for item in receipt.outcomes} == expected_deletes
    assert all(item.outcome == "unlinked" for item in receipt.outcomes)
    for identity in expected_deletes:
        assert not (root / objects[identity].payload_ref.relative_path).exists()
    for identity, raw in exact_before.items():
        assert (
            root / objects[identity].payload_ref.relative_path
        ).read_bytes() == raw
    assert source_path.read_bytes() == _GENERATED_ASCII
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "after-inspect",
        ["artifacts-inspect", "--store", str(root)],
    )
    assert code == 0
    after = c.StoreSnapshotV1.from_json(captured.out)
    assert not after.blockers
    assert protected <= set(after.present_object_ids)
    assert (
        set(before.present_object_ids) - set(after.present_object_ids)
        == expected_deletes
    )
    assert {item.object_id for item in after.tombstones} == expected_deletes


def test_actual_stale_plan_refusal_after_new_ingestion(tmp_path, capsys):
    from histdatacom.artifact_retention import storage

    root, source_path, _, _, _ = _actual_store(tmp_path)
    plan_path = tmp_path / "stale-plan.json"
    code, _ = _actual_cli(
        tmp_path,
        capsys,
        "before-mutation-plan",
        ["artifacts-plan", "--store", str(root), "--output", str(plan_path)],
    )
    assert code == 0
    source = storage.ingest_ascii_tick_source(
        root, source_path, symbol="EURUSD", period="202001"
    )
    current = storage.inspect_retention_store(root)
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "stale-apply",
        ["artifacts-apply", "--store", str(root), "--plan", str(plan_path)],
    )
    assert code != 0
    assert not captured.out
    assert "stale" in captured.err.lower() or "snapshot" in captured.err.lower()
    assert storage.inspect_retention_store(root).to_json() == current.to_json()
    assert source.primary_object_id in current.present_object_ids
    assert source_path.read_bytes() == _GENERATED_ASCII


def test_actual_legacy_yaml_apply_never_flows_into_artifact_cleanup(
    tmp_path, capsys
):
    root, source_path, _, _, _ = _actual_store(tmp_path)
    legacy = tmp_path / "data" / "ascii" / "T" / "EURUSD" / "2020" / "01"
    legacy.mkdir(parents=True)
    loose = legacy / "DAT_ASCII_EURUSD_T_202001.csv"
    loose.write_bytes(_GENERATED_ASCII)
    legacy_config = _config(
        tmp_path,
        f"  command: sources\n  data_directory: {tmp_path / 'data'}\n"
        "  apply: true\n  json: true\n",
    )
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "explicit-artifact-override",
        [*legacy_config, "artifacts-inspect", "--store", str(root)],
    )
    assert code == 0
    assert c.StoreSnapshotV1.from_json(captured.out).descriptors == ()
    assert loose.read_bytes() == source_path.read_bytes() == _GENERATED_ASCII
    unscoped = _config(tmp_path, "  apply: true\n")
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "unscoped-apply-refused",
        [*unscoped, "artifacts-plan", "--store", str(root)],
    )
    assert code != 0 and not captured.out
    assert loose.read_bytes() == _GENERATED_ASCII


def test_actual_interrupted_apply_emits_error_evidence_not_success_receipt(
    tmp_path, capsys, monkeypatch
):
    from histdatacom.artifact_retention.lifecycle_contracts import (
        CollectionInterruptedEvidenceV1,
    )
    from histdatacom.artifact_retention.secure_fs import StoreSession

    root, source_path, source, _, _ = _actual_store(tmp_path, cache_count=1)
    assert source is not None
    plan_path = tmp_path / "interrupt-plan.json"
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "interrupt-plan",
        ["artifacts-plan", "--store", str(root), "--output", str(plan_path)],
    )
    assert code == 0
    plan = c.CollectionPlanV1.from_json(captured.out)
    candidates = tuple(
        item.object_id for item in plan.decisions if item.action == "delete"
    )
    assert (
        len(candidates) == 2
    )  # one actual cache and its completed work record
    original_unlink = StoreSession._unlink_payload
    effects = []

    def actual_unlink_then_fail(self, observation):
        # Inject a genuine post-effect filesystem failure, not a fake apply
        # result or receipt. The real journal/exception path must report it.
        original_unlink(self, observation)
        effects.append(observation.relative_path)
        raise OSError("synthetic post-unlink durability boundary failure")

    monkeypatch.setattr(
        StoreSession, "_unlink_payload", actual_unlink_then_fail
    )
    output = tmp_path / "actual-interruption.json"
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "interrupted-apply",
        [
            "artifacts-apply",
            "--store",
            str(root),
            "--plan",
            str(plan_path),
            "--output",
            str(output),
        ],
    )
    assert code != 0
    evidence = CollectionInterruptedEvidenceV1.from_json(captured.out)
    assert evidence.status == "interrupted"
    assert evidence.plan_id == plan.artifact_id
    assert len(effects) == 1 and not (root / effects[0]).exists()
    assert tuple(item.object_id for item in evidence.outcomes) == candidates
    assert [item.outcome for item in evidence.outcomes] == [
        "indeterminate",
        "not_attempted",
    ]
    assert all(item.size_bytes_unlinked == 0 for item in evidence.outcomes)
    assert evidence.receipt is not None
    assert evidence.receipt.status == "indeterminate"
    assert evidence.receipt.protected_replay != "verified"
    assert output.read_bytes() == captured.out.encode("ascii")
    with pytest.raises(ValueError):
        c.CollectionReceiptV1.from_json(captured.out)
    assert source_path.read_bytes() == _GENERATED_ASCII


def test_actual_post_receipt_validation_failure_preserves_unaccepted_identity(
    tmp_path, capsys, monkeypatch
):
    """Persisted proposed bytes are not an accepted complete CLI outcome."""
    from histdatacom.artifact_retention import apply as apply_module
    from histdatacom.artifact_retention.lifecycle_contracts import (
        CollectionInterruptedEvidenceV1,
    )

    root, _, _, _, _ = _actual_store(tmp_path)
    plan_path = tmp_path / "post-receipt-plan.json"
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "post-receipt-plan",
        ["artifacts-plan", "--store", str(root), "--output", str(plan_path)],
    )
    assert code == 0
    plan = c.CollectionPlanV1.from_json(captured.out)
    original_check = apply_module._unchanged_prefix
    failures = []

    def fail_after_actual_receipt_publication(state):
        original_check(state)
        receipts = state.of_type(c.CollectionReceiptV1)
        if receipts:
            failures.append(receipts[0])
            raise ValueError(
                "synthetic validation failure after actual receipt write"
            )

    monkeypatch.setattr(
        apply_module, "_unchanged_prefix", fail_after_actual_receipt_publication
    )
    code, captured = _actual_cli(
        tmp_path,
        capsys,
        "post-receipt-apply",
        ["artifacts-apply", "--store", str(root), "--plan", str(plan_path)],
    )
    assert code != 0
    evidence = CollectionInterruptedEvidenceV1.from_json(captured.out)
    assert len(failures) == 1
    assert evidence.status == "interrupted"
    assert evidence.plan_id == plan.artifact_id
    assert evidence.receipt is None
    assert evidence.receipt_persisted is True
    assert evidence.unaccepted_receipt_id == failures[0].artifact_id
    retained = [
        c.CollectionReceiptV1.from_json(path.read_text(encoding="ascii"))
        for path in (root / "receipts").glob("*.json")
        if json.loads(path.read_text(encoding="ascii")).get("schema_version")
        == c.CollectionReceiptV1.SCHEMA
    ]
    assert retained == failures
    with pytest.raises(ValueError):
        c.CollectionReceiptV1.from_json(captured.out)

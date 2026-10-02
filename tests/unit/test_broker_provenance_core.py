"""Generated reference chain and adversarial IO; no provider/network work."""

import hashlib
import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceConformanceStatus as Conformance,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceEntryKind as Kind,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceHeaderV1 as Header,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceLinkV1 as Link,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceNativeFamily as Family,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenancePolicyV1 as Policy,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceTerminalV1 as Terminal,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceVerificationReason as Reason,
)
from histdatacom.broker_plugin_provenance import (
    link_provenance_entry,
    make_provenance_entry,
    provenance_header_digest,
    verify_provenance_chain,
)
from histdatacom.broker_plugin_provenance._wire import (
    canonical_json,
    identity,
    load_json,
)
from histdatacom.broker_plugin_provenance.chain import _ChainState
from histdatacom.broker_plugin_provenance.storage import (
    _ProvenanceWriter,
    _read_provenance,
)


def generated_header(**changes):
    values = {
        "family": Family.LEGACY_CAPTURE_V1,
        "capture_id": "generated-capture",
        "native_header_id": "generated-session",
        "native_header_sha256": "1" * 64,
        "plugin_id": "generated-adapter",
        "plugin_version": "1.0.0",
        "provider_id": "generated-provider",
        "feed_id": None,
        "configuration_id": "generated-configuration",
        "configuration_sha256": "2" * 64,
        "distribution_name": None,
        "distribution_version": None,
        "implementation_sha256": None,
        "registration_sha256": None,
        "sdk_version": None,
        "event_schema_version": "histdatacom.broker-capture-event.v1",
        "permission_manifest_id": None,
        "permission_context_id": None,
        "permission_decision_id": None,
        "permission_grant_id": None,
        "provider_decision_id": "generated-provider-decision",
        "host_version": "3.0.0",
        "host_python_version": "3.10.19",
        "environment_id": "generated-environment",
        "started_at_utc_ns": 100,
        "started_at_monotonic_ns": 200,
        "conformance_version": None,
        "conformance_status": Conformance.NOT_APPLICABLE,
        "conformance_receipt_id": None,
        "policy": Policy(checkpoint_interval=2, max_entries=1000),
    }
    values.update(changes)
    return Header(**values)


def generated_terminal(header, **changes):
    values = {
        "header_id": header.artifact_id,
        "stopped_at_utc_ns": 300,
        "stopped_at_monotonic_ns": 400,
        "native_manifest_id": "generated-manifest",
        "native_manifest_sha256": "3" * 64,
        "health_audit_id": "generated-health-audit",
        "health_audit_sha256": "4" * 64,
        "permission_execution_id": None,
        "permission_execution_sha256": None,
        "native_complete": True,
        "observations_complete": True,
    }
    values.update(changes)
    return Terminal(**values)


def generated_chain(header=None, terminal_changes=None, values=None):
    header = header or generated_header()
    state = _ChainState(header)
    links, checkpoints = [], []
    values = values or [
        (Kind.ADMITTED_INGRESS, 0, "first", None),
        (Kind.NATIVE_RECORD, 0, "first", "record-0"),
        (Kind.HOST_OBSERVATION, 0, "persisted", None),
    ]
    for kind, epoch, value, native_id in values:
        entry = make_provenance_entry(
            header,
            state.count,
            epoch,
            kind,
            canonical_json({"value": value}),
            native_record_id=native_id,
        )
        link = link_provenance_entry(state.root, entry)
        links.append(link)
        if state.accept(link):
            checkpoint = state.expected_checkpoint()
            checkpoints.append(checkpoint)
            state.accept_checkpoint(checkpoint)
    terminal = generated_terminal(header, **(terminal_changes or {}))
    entry = make_provenance_entry(
        header,
        state.count,
        state.max_epoch,
        Kind.TERMINAL,
        terminal.to_json(),
    )
    link = link_provenance_entry(state.root, entry)
    links.append(link)
    assert state.accept(link)
    checkpoint = state.expected_checkpoint()
    checkpoints.append(checkpoint)
    state.accept_checkpoint(checkpoint)
    return header, links, checkpoints, state.make_seal()


def verified(chain):
    header, links, checkpoints, seal = chain
    return verify_provenance_chain(
        header,
        links,
        checkpoints,
        seal,
        expected_header_id=header.artifact_id,
        expected_seal_id=seal.artifact_id,
    )


def write_generated(
    directory, *, guard=lambda _: None, authorize=lambda _: None
):
    header = generated_header()
    writer = _ProvenanceWriter(
        directory,
        header,
        guard_text=guard,
        authorize_artifact=authorize,
        metadata={"native-header.json": '{"generated":true}'},
    )
    writer.append(Kind.ADMITTED_INGRESS, 0, '{"value":"first"}')
    writer.append(
        Kind.NATIVE_RECORD, 0, '{"value":"first"}', native_record_id="record-0"
    )
    seal = writer.finish(generated_terminal(header), epoch=0)
    return header, seal


def read_generated(directory, header, seal, **changes):
    values = {
        "authorize_artifact": lambda _: None,
        "guard_text": lambda _: None,
        "expected_header_id": header.artifact_id,
        "expected_seal_id": seal.artifact_id,
    }
    values.update(changes)
    return _read_provenance(directory, **values)


def test_independent_sha256_reference_and_canonical_roundtrip():
    chain = generated_chain()
    header, links, _checkpoints, seal = chain
    assert Header.from_json(header.to_json()) == header
    expected = hashlib.sha256(header.to_json().encode("ascii")).digest()
    assert expected.hex() == provenance_header_digest(header)
    for link in links:
        text = json.dumps(
            link.entry.to_dict(),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        assert link.previous_sha256 == expected.hex()
        expected = hashlib.sha256(expected + text).digest()
        assert link.sha256 == expected.hex()
        assert Link.from_json(link.to_json()) == link
    assert seal.root_sha256 == expected.hex()
    result = verified(chain)
    assert result.complete and result.anchored and result.structurally_complete
    assert result.header == header and result.seal == seal
    assert result.entry_count == 4 and result.checkpoint_count == 2
    assert verified(generated_chain()) == result


def _reference_wire(artifact):
    payload = artifact.to_payload()
    schema = artifact.schema_version()
    return canonical_json(
        {
            "schema_version": schema,
            "artifact_id": identity(
                artifact.KIND, {"schema_version": schema, "payload": payload}
            ),
            "payload": payload,
        }
    )


def test_single_preflight_serializer_preserves_reference_bytes_and_identities():
    header, links, checkpoints, seal = generated_chain()
    artifacts = (
        header,
        header.policy,
        *links,
        *checkpoints,
        seal,
        seal.terminal,
    )
    for artifact in artifacts:
        expected = _reference_wire(artifact)
        assert artifact.to_json() == expected
        assert artifact.artifact_id == json.loads(expected)["artifact_id"]
        assert type(artifact).from_json(expected).to_json() == expected


@pytest.mark.parametrize(
    "bound,limits",
    [
        ("MAX_NODES", range(1, 100)),
        ("MAX_DEPTH", range(1, 7)),
        ("MAX_ITEMS", range(1, 50)),
        ("MAX_BYTES", range(2550, 3100, 7)),
    ],
)
def test_single_preflight_preserves_exact_reference_envelope_bounds(
    monkeypatch, bound, limits
):
    from histdatacom.broker_plugin_provenance import _wire

    header = generated_header()
    for limit in limits:
        monkeypatch.setattr(_wire, bound, limit)
        try:
            expected = _reference_wire(header)
        except ValueError:
            with pytest.raises(ValueError):
                header.to_json()
        else:
            assert header.to_json() == expected


@pytest.mark.parametrize(
    "corrupt", [float("nan"), float("inf"), 2**64, object()]
)
def test_serialization_rechecks_corrupted_frozen_fields_on_every_call(corrupt):
    header = generated_header()
    header.to_json()
    assert header.artifact_id
    object.__setattr__(header, "started_at_utc_ns", corrupt)
    for encode in (header.to_json, lambda: header.artifact_id):
        with pytest.raises(ValueError):
            encode()


def test_verifier_rechecks_corrupted_frozen_exact_types_after_serialization():
    header, links, checkpoints, seal = generated_chain()
    links[0].to_json()
    object.__setattr__(links[0].entry, "sequence", False)
    assert (
        verified((header, links, checkpoints, seal)).reason
        is Reason.INVALID_ENTRY
    )


@pytest.mark.parametrize("anchor", ["neither", "header", "seal"])
def test_self_consistency_is_not_external_identity(anchor):
    header, links, checkpoints, seal = generated_chain()
    result = verify_provenance_chain(
        header,
        links,
        checkpoints,
        seal,
        expected_header_id=header.artifact_id if anchor == "header" else None,
        expected_seal_id=seal.artifact_id if anchor == "seal" else None,
    )
    assert result.reason is Reason.UNANCHORED
    assert result.structurally_complete and not result.complete


@pytest.mark.parametrize("operation", ["swap", "insert", "delete", "mutate"])
def test_middle_record_tampering_is_detected(operation):
    header, links, checkpoints, seal = generated_chain()
    if operation == "swap":
        links[0], links[1] = links[1], links[0]
    elif operation == "insert":
        links.insert(1, links[0])
    elif operation == "delete":
        del links[1]
    else:
        links[1] = replace(
            links[1],
            entry=replace(links[1].entry, payload_chunks=('{"value":"evil"}',)),
        )
    result = verified((header, links, checkpoints, seal))
    assert result.reason in (Reason.SEQUENCE, Reason.CHAIN_DIGEST)
    assert not result.complete


@pytest.mark.parametrize(
    "field,value",
    [
        ("plugin_version", "2.0.0"),
        ("configuration_sha256", "f" * 64),
        ("provider_decision_id", "other-provider-decision"),
        ("environment_id", "other-runtime"),
    ],
)
def test_recomputed_whole_chain_cannot_replace_an_external_anchor(field, value):
    original = generated_chain()
    altered = generated_chain(replace(original[0], **{field: value}))
    result = verify_provenance_chain(
        *altered,
        expected_header_id=original[0].artifact_id,
        expected_seal_id=original[3].artifact_id,
    )
    assert result.reason is Reason.ANCHOR_MISMATCH


@pytest.mark.parametrize("operation", ["missing", "extra", "order", "root"])
def test_checkpoint_inventory_is_exact(operation):
    header, links, checkpoints, seal = generated_chain()
    if operation == "missing":
        checkpoints.pop()
    elif operation == "extra":
        checkpoints.append(checkpoints[-1])
    elif operation == "order":
        checkpoints.reverse()
    else:
        checkpoints[0] = replace(checkpoints[0], root_sha256="0" * 64)
    assert (
        verified((header, links, checkpoints, seal)).reason is Reason.CHECKPOINT
    )


def test_interleaved_old_epoch_is_retained_but_native_epoch_cannot_regress():
    values = [
        (Kind.ADMITTED_INGRESS, 0, "old", None),
        (Kind.ADMITTED_INGRESS, 1, "reconnect", None),
        (Kind.HOST_OBSERVATION, 0, "old-persist", None),
        (Kind.NATIVE_RECORD, 0, "clock-correction", "record-0"),
        (Kind.NATIVE_RECORD, 1, "reconnect", "record-1"),
    ]
    assert verified(generated_chain(values=values)).complete
    with pytest.raises(ValueError, match="epoch"):
        generated_chain(
            values=values + [(Kind.NATIVE_RECORD, 0, "old", "record-2")]
        )


@pytest.mark.parametrize("epoch", [1, 2, 999999])
def test_epoch_cannot_start_at_unknown_nonzero_value(epoch):
    with pytest.raises(ValueError, match="epoch"):
        generated_chain(values=[(Kind.ADMITTED_INGRESS, epoch, "first", None)])


@pytest.mark.parametrize("field", ["native_complete", "observations_complete"])
def test_partial_terminal_never_becomes_complete(field):
    result = verified(generated_chain(terminal_changes={field: False}))
    assert result.reason is Reason.PARTIAL
    assert not result.complete and not result.structurally_complete


def test_unsealed_prefix_is_partial():
    header, links, checkpoints, _ = generated_chain()
    result = verify_provenance_chain(header, links[:2], checkpoints[:1], None)
    assert result.reason is Reason.PARTIAL


@pytest.mark.parametrize(
    "filename",
    [
        "header.json",
        "chain.jsonl",
        "checkpoints.jsonl",
        "seal.json",
        "native-header.json",
    ],
)
def test_fifo_substitution_refuses_without_blocking(tmp_path, filename):
    directory = tmp_path / "generated-provenance"
    write_generated(directory)
    path = directory / filename
    path.unlink()
    os.mkfifo(path)
    script = (
        "import sys\n"
        "from histdatacom.broker_plugin_provenance.storage import _read_provenance\n"
        "try:\n"
        " _read_provenance(sys.argv[1], authorize_artifact=lambda _: None, guard_text=lambda _: None)\n"
        "except ValueError as error:\n"
        " assert 'unsafe' in str(error), str(error)\n"
        "else:\n"
        " raise AssertionError('FIFO was accepted')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(directory)],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "text",
    [
        '{"a":1,"a":2}',
        '{"a":NaN}',
        '{"a":Infinity}',
        '{ "a":1}',
        '{"a":1}\n',
        "[]",
        '{"a":1.0, "b":2}',
    ],
)
def test_nested_payload_is_strict_canonical_object(text):
    with pytest.raises(ValueError):
        make_provenance_entry(
            generated_header(), 0, 0, Kind.ADMITTED_INGRESS, text
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("started_at_utc_ns", True),
        ("started_at_monotonic_ns", -1),
        ("native_header_sha256", "A" * 64),
        ("algorithm", "sha1"),
        ("sdk_version", "invented"),
        ("permission_grant_id", "invented"),
        ("conformance_status", Conformance.CANDIDATE_QUALIFIED),
    ],
)
def test_invalid_header_facts_are_closed(field, value):
    with pytest.raises(ValueError):
        generated_header(**{field: value})


def test_payload_alias_amplification_and_depth_are_bounded():
    alias = ["a"]
    for _ in range(18):
        alias = [alias, alias]
    with pytest.raises(ValueError):
        canonical_json({"alias": alias})
    with pytest.raises(ValueError):
        load_json("[" * 30 + "0" + "]" * 30)


def test_private_storage_roundtrip_and_metadata(tmp_path):
    directory = tmp_path / "generated"
    header, seal = write_generated(directory)
    result = read_generated(directory, header, seal)
    assert result.verification.complete
    assert result.metadata == {"native-header.json": '{"generated":true}'}
    assert not result.partial_tail
    with pytest.raises(TypeError):
        result.metadata["new.json"] = "{}"


def test_directory_inventory_stops_before_materializing_unbounded_names(
    tmp_path, monkeypatch
):
    directory = tmp_path / "generated"
    header, seal = write_generated(directory)
    for index in range(80):
        (directory / f"extra-{index}.json").touch()
    real_scandir = os.scandir
    seen = []

    @contextmanager
    def measured_scandir(fd):
        with real_scandir(fd) as entries:

            def measured_entries():
                for entry in entries:
                    seen.append(entry.name)
                    yield entry

            yield measured_entries()

    monkeypatch.setattr(os, "scandir", measured_scandir)
    with pytest.raises(ValueError, match="inventory exceeds bound"):
        read_generated(directory, header, seal)
    assert len(seen) == 21


@pytest.mark.parametrize("filename", ["chain.jsonl", "checkpoints.jsonl"])
def test_retained_partial_tail_never_verifies_complete(tmp_path, filename):
    directory = tmp_path / "generated"
    header, seal = write_generated(directory)
    path = directory / filename
    path.write_bytes(path.read_bytes()[:-1])
    result = read_generated(directory, header, seal)
    assert result.partial_tail
    assert result.verification.reason is Reason.PARTIAL
    assert not result.verification.complete


@pytest.mark.parametrize(
    "filename", ["header.json", "chain.jsonl", "seal.json"]
)
def test_symlink_file_is_not_read(tmp_path, filename):
    directory = tmp_path / "generated"
    header, seal = write_generated(directory)
    original = directory / filename
    target = tmp_path / "generated-copy"
    target.write_bytes(original.read_bytes())
    original.unlink()
    original.symlink_to(target)
    with pytest.raises(OSError):
        read_generated(directory, header, seal)


def test_hardlinked_file_is_not_read(tmp_path):
    directory = tmp_path / "generated"
    header, seal = write_generated(directory)
    os.link(directory / "chain.jsonl", tmp_path / "generated-link")
    with pytest.raises(ValueError, match="unsafe"):
        read_generated(directory, header, seal)


def test_symlink_directory_and_existing_output_are_not_overwritten(tmp_path):
    directory = tmp_path / "generated"
    header, seal = write_generated(directory)
    link = tmp_path / "generated-alias"
    link.symlink_to(directory, target_is_directory=True)
    with pytest.raises(OSError):
        read_generated(link, header, seal)
    with pytest.raises(FileExistsError):
        write_generated(directory)
    assert read_generated(directory, header, seal).verification.complete


def test_parent_alias_is_canonicalized_without_following_directory_or_file_leaf(
    tmp_path,
):
    physical = tmp_path / "physical"
    physical.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(physical, target_is_directory=True)
    requested = alias / "generated"
    header, seal = write_generated(requested)
    assert read_generated(requested, header, seal).verification.complete
    assert read_generated(
        physical / "generated", header, seal
    ).verification.complete

    directory_leaf = alias / "directory-leaf"
    directory_leaf.symlink_to(physical / "generated", target_is_directory=True)
    with pytest.raises(FileExistsError):
        write_generated(directory_leaf)
    with pytest.raises(OSError):
        read_generated(directory_leaf, header, seal)

    original = physical / "generated" / "seal.json"
    retained = physical / "original-seal.json"
    original.rename(retained)
    original.symlink_to(retained)
    with pytest.raises(OSError):
        read_generated(requested, header, seal)
    assert retained.read_text() == seal.to_json()


def test_guard_revocation_precedes_initial_directory_creation(tmp_path):
    revoked = False

    def guard(_):
        nonlocal revoked
        revoked = True

    def authorize(_):
        if revoked:
            raise PermissionError("generated-revocation")

    directory = tmp_path / "generated"
    with pytest.raises(PermissionError, match="generated-revocation"):
        write_generated(directory, guard=guard, authorize=authorize)
    assert not directory.exists()


def test_append_guard_revocation_does_not_retain_entry_or_seal(tmp_path):
    revoked = False

    def guard(text):
        nonlocal revoked
        if '"schema_version":"histdatacom.broker-provenance-link.v1"' in text:
            revoked = True

    def authorize(_):
        if revoked:
            raise PermissionError("generated-revocation")

    directory = tmp_path / "generated"
    header = generated_header()
    writer = _ProvenanceWriter(
        directory, header, guard_text=guard, authorize_artifact=authorize
    )
    with pytest.raises(PermissionError):
        writer.append(Kind.ADMITTED_INGRESS, 0, '{"value":"generated"}')
    assert (directory / "chain.jsonl").read_bytes() == b""
    assert not (directory / "seal.json").exists()
    with pytest.raises(ValueError, match="appendable"):
        writer.append(Kind.ADMITTED_INGRESS, 0, "{}")


def test_reauthorization_occurs_before_each_fsync(tmp_path, monkeypatch):
    authorized = False
    count = 0
    real_fsync = os.fsync

    def authorize(_):
        nonlocal authorized
        authorized = True

    def fsync(fd):
        nonlocal authorized, count
        assert authorized
        authorized = False
        count += 1
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    write_generated(tmp_path / "generated", authorize=authorize)
    assert count >= 10


def test_read_guard_revocation_prevents_result_release(tmp_path):
    directory = tmp_path / "generated"
    header, seal = write_generated(directory)
    revoked = False

    def guard(text):
        nonlocal revoked
        if '"schema_version":"histdatacom.broker-provenance-seal.v1"' in text:
            revoked = True

    def authorize(_):
        if revoked:
            raise PermissionError("generated-revocation")

    with pytest.raises(PermissionError):
        read_generated(
            directory,
            header,
            seal,
            guard_text=guard,
            authorize_artifact=authorize,
        )


def test_terminal_does_not_introduce_unobserved_epoch():
    header, links, checkpoints, _ = generated_chain()
    state = _ChainState(header)
    for link in links[:-1]:
        if state.accept(link):
            state.accept_checkpoint(checkpoints[0])
    entry = replace(links[-1].entry, epoch=1)
    with pytest.raises(ValueError, match="epoch"):
        state.accept(link_provenance_entry(state.root, entry))


@pytest.mark.parametrize(
    "field", ["native_manifest_sha256", "health_audit_sha256"]
)
def test_recomputed_terminal_cannot_replace_expected_seal(field):
    original = generated_chain()
    altered = generated_chain(terminal_changes={field: "f" * 64})
    result = verify_provenance_chain(
        *altered,
        expected_header_id=original[0].artifact_id,
        expected_seal_id=original[3].artifact_id,
    )
    assert result.reason is Reason.ANCHOR_MISMATCH


def test_closed_kind_and_forged_bool_checkpoint_are_rejected():
    header, links, checkpoints, seal = generated_chain()
    with pytest.raises(ValueError):
        replace(links[0].entry, kind="plugin_defined")
    object.__setattr__(checkpoints[0], "epoch", False)
    assert (
        verified((header, links, checkpoints, seal)).reason is Reason.CHECKPOINT
    )


@pytest.mark.parametrize(
    "policy",
    [
        Policy(max_entries=2, checkpoint_interval=2),
        Policy(max_entry_bytes=256),
        Policy(max_capture_bytes=1024),
    ],
)
def test_capture_policy_limits_refuse_without_truncating_to_success(policy):
    with pytest.raises(ValueError, match="limit"):
        generated_chain(generated_header(policy=policy))


def test_payload_chunks_preserve_large_exact_native_bytes():
    text = canonical_json({"generated": "a" * 65_000})
    entry = make_provenance_entry(
        generated_header(), 0, 0, Kind.ADMITTED_INGRESS, text
    )
    assert len(entry.payload_chunks) == 2 and entry.payload_json == text
    with pytest.raises(ValueError, match="chunks"):
        replace(entry, payload_chunks=(text[:2], text[2:]))


def test_storage_inventory_mutation_during_late_guard_is_refused(tmp_path):
    directory = tmp_path / "generated"
    header, seal = write_generated(directory)

    def guard(text):
        if '"schema_version":"histdatacom.broker-provenance-seal.v1"' in text:
            with (directory / "chain.jsonl").open("ab") as stream:
                stream.write(b"trailing-generated-fragment")

    with pytest.raises(ValueError, match="changed during replay"):
        read_generated(directory, header, seal, guard_text=guard)


def test_private_writer_reentry_and_cross_thread_calls_are_refused(tmp_path):
    writer = None

    def guard(text):
        if '"schema_version":"histdatacom.broker-provenance-link.v1"' in text:
            assert writer is not None
            with pytest.raises(ValueError, match="nonreentrant"):
                writer.append(Kind.ADMITTED_INGRESS, 0, "{}")

    header = generated_header()
    writer = _ProvenanceWriter(
        tmp_path / "generated",
        header,
        guard_text=guard,
        authorize_artifact=lambda _: None,
    )
    failures = []

    def other_thread():
        try:
            writer.append(Kind.ADMITTED_INGRESS, 0, "{}")
        except ValueError as exc:
            failures.append(str(exc))

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join(2)
    assert not thread.is_alive() and failures
    assert writer.entry_count == 0
    writer.append(Kind.ADMITTED_INGRESS, 0, "{}")
    seal = writer.finish(generated_terminal(header), epoch=0)
    assert seal.entry_count == 2


def test_closed_writer_cannot_append_after_seal(tmp_path):
    header = generated_header()
    writer = _ProvenanceWriter(
        tmp_path / "generated",
        header,
        guard_text=lambda _: None,
        authorize_artifact=lambda _: None,
    )
    writer.finish(generated_terminal(header), epoch=0)
    with pytest.raises(ValueError, match="appendable"):
        writer.append(Kind.ADMITTED_INGRESS, 0, "{}")


def test_source_only_provenance_import_does_not_import_runtime_or_third_party():
    source = Path(__file__).resolve().parents[2] / "src"
    script = f"""
import sys
sys.path.append({str(source)!r})
import histdatacom.broker_plugin_provenance as core
assert core.BrokerProvenanceHeaderV1
assert 'histdatacom.broker_plugin_provenance.native' not in sys.modules
assert 'histdatacom.broker_plugin_provenance.runtime' not in sys.modules
assert 'histdatacom.broker_capture' not in sys.modules
assert 'polars' not in sys.modules
assert 'pandas' not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-B", "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr

"""Actual generated host captures and anchored offline native replay."""

from dataclasses import replace
import json
import shutil

import pytest

from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceHeaderV1,
    BrokerProvenanceSealV1,
    require_legacy_capture_provenance,
)
from tests.fixtures.broker_provider_policy import generated_provider_scope
from tests.unit.test_broker_host_health_runtime_legacy import capture


@pytest.fixture(scope="module")
def native_capture(tmp_path_factory):
    root = tmp_path_factory.mktemp("generated-provenance-native")
    request, result = capture(root)
    return root, request, result


def verify(root, request, result, *, expected=True):
    with generated_provider_scope(request):
        return require_legacy_capture_provenance(
            root,
            result.manifest,
            provider_request=request,
            expected_root=result.provenance if expected else None,
        )


def test_real_native_capture_replays_against_external_seal(native_capture):
    root, request, result = native_capture
    unanchored = verify(root, request, result, expected=False)
    assert unanchored.structurally_complete and not unanchored.anchored
    first = verify(root, request, result)
    second = verify(root, request, result)
    assert first == second
    assert first.complete and first.anchored
    assert first.seal == result.provenance
    assert first.header.permission_grant_id is None
    assert first.header.sdk_version is None
    assert first.header.distribution_name is None
    assert first.seal.terminal.native_manifest_id == result.manifest.manifest_id
    assert first.seal.terminal.health_audit_id == result.audit.artifact_id


@pytest.mark.parametrize(
    "change",
    [
        "delete",
        "insert",
        "reorder",
        "mutate",
        "tail",
        "missing-seal",
        "foreign",
    ],
)
def test_native_reader_refuses_tampered_or_interrupted_chain(
    native_capture, tmp_path, change
):
    original, request, result = native_capture
    copied = tmp_path / "copy"
    shutil.copytree(original, copied)
    directory = copied / result.provenance_directory.name
    path = directory / "chain.jsonl"
    lines = path.read_text().splitlines()
    if change == "delete":
        del lines[1]
    elif change == "insert":
        lines.insert(1, lines[1])
    elif change == "reorder":
        lines[1], lines[2] = lines[2], lines[1]
    elif change == "mutate":
        row = json.loads(lines[1])
        row["sha256"] = "0" * 64
        lines[1] = json.dumps(row, sort_keys=True, separators=(",", ":"))
    elif change == "tail":
        lines.append('{"unfinished":')
    elif change == "missing-seal":
        (directory / "seal.json").unlink()
    else:
        (directory / "foreign.json").write_text("{}")
    path.write_text("\n".join(lines) + ("" if change == "tail" else "\n"))
    with pytest.raises(ValueError):
        verify(copied, request, result)


@pytest.mark.parametrize(
    "field,value",
    [
        ("plugin_version", "9.0.0"),
        ("configuration_sha256", "0" * 64),
        ("native_header_sha256", "0" * 64),
        ("provider_decision_id", "substituted-policy:sha256:" + "0" * 64),
        ("host_version", "9.0.0"),
        ("environment_id", "substituted-environment:sha256:" + "0" * 64),
    ],
)
def test_native_reader_binds_header_software_authority_and_environment(
    native_capture, tmp_path, field, value
):
    original, request, result = native_capture
    copied = tmp_path / "copy"
    shutil.copytree(original, copied)
    header_path = copied / result.provenance_directory.name / "header.json"
    header = BrokerProvenanceHeaderV1.from_json(header_path.read_text())
    header_path.write_text(replace(header, **{field: value}).to_json())
    with pytest.raises(ValueError):
        verify(copied, request, result)


def test_native_reader_cannot_accept_another_external_root(native_capture):
    root, request, result = native_capture
    substituted = replace(result.provenance, root_sha256="0" * 64)
    assert (
        BrokerProvenanceSealV1.from_json(substituted.to_json()) == substituted
    )
    with generated_provider_scope(request), pytest.raises(ValueError):
        require_legacy_capture_provenance(
            root,
            result.manifest,
            provider_request=request,
            expected_root=substituted,
        )


def test_native_reader_refuses_retained_proof_without_current_rights(
    native_capture,
):
    root, request, result = native_capture
    with pytest.raises(
        ValueError, match="current_process_policy_scope_required"
    ):
        require_legacy_capture_provenance(
            root,
            result.manifest,
            provider_request=request,
            expected_root=result.provenance,
        )


def test_provenance_sidecar_cannot_be_replaced_with_symlink(
    native_capture, tmp_path
):
    original, request, result = native_capture
    copied = tmp_path / "copy"
    shutil.copytree(original, copied)
    directory = copied / result.provenance_directory.name
    chain = directory / "chain.jsonl"
    chain.unlink()
    chain.symlink_to(result.provenance_directory / "chain.jsonl")
    with pytest.raises((ValueError, OSError)):
        verify(copied, request, result)


def test_forbidden_raw_hash_never_leaks_its_derived_ingress_identity(tmp_path):
    from histdatacom.broker_capture import SequenceBrokerCaptureAdapterV1
    from histdatacom.broker_plugin_health.runtime_legacy import (
        capture_legacy_with_host_health,
    )
    from tests.fixtures.broker_provider_policy import (
        generated_legacy_request,
        legacy_policy_inputs,
    )
    from tests.unit.test_broker_host_health_runtime_legacy import TickClock

    inputs = legacy_policy_inputs()
    original = generated_legacy_request(inputs.session)
    request = replace(
        original,
        output_contract=replace(
            original.output_contract, allow_raw_hashes=False
        ),
    )
    raw_hash = "ab" * 32
    message = replace(
        inputs.messages[0], raw_message_sha256=raw_hash, message_id=""
    )
    adapter = SequenceBrokerCaptureAdapterV1(
        inputs.session.adapter_id,
        inputs.session.adapter_version,
        (message,),
    )
    with (
        generated_provider_scope(request),
        pytest.raises(ValueError, match="disabled raw hash provenance"),
    ):
        capture_legacy_with_host_health(
            tmp_path,
            provider_request=request,
            adapter=adapter,
            storage_policy=inputs.storage_policy,
            symbols=("EURUSD",),
            clock=TickClock(inputs.session),
        )
    retained = b"".join(
        path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    )
    assert raw_hash.encode() not in retained
    assert message.message_id.encode() not in retained
    assert not tuple(tmp_path.glob("*-provenance/seal.json"))
    assert not tuple(tmp_path.glob("*-host-health/audit.json"))

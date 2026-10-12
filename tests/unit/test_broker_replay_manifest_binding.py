"""Exact source-manifest binding with two coherent synthetic journals.

These tests exercise real lifecycle storage replay and current rights. The
compatibility test supplies a typed, structurally verified A proof at the
already-verified-provenance boundary; it is not a physical acquisition or an
end-to-end provenance qualification. Neither replay nor its records are mocked.
"""

from dataclasses import fields, replace
import hashlib
import json
import time

import pytest

from histdatacom.broker_capture import lifecycle_fingerprints as projection
from histdatacom.broker_capture.fingerprint_v3 import (
    BrokerFingerprintLifecycleRootV1,
    BrokerLifecycleFingerprintFitConfigV1,
)
from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    validate_broker_capability_event,
    validate_broker_metadata,
)
from histdatacom.broker_plugin_compatibility import provenance as compatibility
from histdatacom.broker_plugin_compatibility.provenance import (
    make_capture_compatibility_declaration,
)
from histdatacom.broker_plugin_health import replay_lifecycle_host_health
from histdatacom.broker_plugin_lifecycle import storage
from histdatacom.broker_plugin_lifecycle.contracts import (
    BrokerLifecycleError,
    BrokerLifecycleIdentityV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePartitionV1,
    BrokerLifecycleReason,
    BrokerLifecycleSessionV1,
)
from histdatacom.broker_plugin_policy import BrokerPolicyError
from histdatacom.broker_plugin_policy.contracts import BrokerPolicyRevocationV1
from histdatacom.broker_plugin_permissions.provenance import (
    BrokerPermissionExecutionV1,
    _chunks,
)
from histdatacom.broker_plugin_provenance import native
from histdatacom.broker_plugin_provenance.native_common import (
    canonical_native_json,
    host_environment_json,
)
from histdatacom.broker_plugin_provenance.runtime import (
    make_native_provenance_header,
)
from histdatacom.broker_plugin_security import execution as security
from histdatacom.broker_plugin_security.contracts import (
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerTrustTier,
)
from tests.fixtures.broker_provider_policy import generated_provider_scope
from tests.unit.test_broker_host_health_replay import sdk_fixture
from tests.unit.test_broker_provenance_core import generated_chain, verified


def _sha(value):
    return hashlib.sha256(value.to_json().encode("ascii")).hexdigest()


def _write(directory, manifest, records):
    directory.mkdir(exist_ok=True)
    data = b"".join(
        record.to_json().encode("ascii") + b"\n" for record in records
    )
    assert len(data) == manifest.partitions[0].byte_count
    assert hashlib.sha256(data).hexdigest() == manifest.partitions[0].sha256
    (directory / "partition-0000.jsonl").write_bytes(data)
    (directory / "manifest.json").write_text(
        manifest.to_json(), encoding="ascii"
    )


@pytest.fixture
def journals(tmp_path):
    values = sdk_fixture(source_order=(1, 2, 3))
    health, observations, a, records_a, request, pm, pc, pd, provider = values
    records_a = tuple(records_a)
    identity_a = BrokerLifecycleIdentityV1.from_json(records_a[2].payload_json)
    session_a = BrokerLifecycleSessionV1.from_json(records_a[3].payload_json)
    metadata_b = replace(
        identity_a.metadata.metadata,
        display_name=identity_a.metadata.metadata.display_name + " source B",
    )
    identity_b = replace(
        identity_a,
        binding=replace(identity_a.binding, metadata_id=metadata_b.artifact_id),
        metadata=validate_broker_metadata(request.plan, metadata_b),
    )
    session_b = replace(
        session_a,
        session=replace(session_a.session, metadata_id=metadata_b.artifact_id),
    )
    records_b = []
    for record in records_a:
        payload = record.payload_json
        if record.kind == "identity":
            payload = identity_b.to_json()
        elif record.kind == "session":
            payload = session_b.to_json()
        elif record.kind == "event":
            admitted = BrokerAdmittedEventV1.from_json(payload)
            event = replace(
                admitted.event,
                session_id=session_b.session.artifact_id,
                quote=(
                    None
                    if admitted.event.quote is None
                    else replace(admitted.event.quote, ask="1.3")
                ),
            )
            payload = validate_broker_capability_event(
                request.plan, event
            ).to_json()
        records_b.append(replace(record, payload_json=payload))
    records_b = tuple(records_b)
    data_b = b"".join(
        record.to_json().encode("ascii") + b"\n" for record in records_b
    )
    old = a.partitions[0]
    partition_b = BrokerLifecyclePartitionV1(
        old.ordinal,
        hashlib.sha256(data_b).hexdigest(),
        len(data_b),
        old.first_sequence,
        old.record_count,
        old.event_count,
    )
    b = replace(a, partitions=(partition_b,))
    assert a.header == b.header
    assert a.header.plan == b.header.plan == request.plan
    assert a.artifact_id != b.artifact_id
    assert identity_a.metadata.metadata.to_json() != metadata_b.to_json()
    directory = tmp_path / "native"
    for manifest, records in ((a, records_a), (b, records_b)):
        assert (
            BrokerLifecycleManifestV1.from_json(manifest.to_json()) == manifest
        )
        _write(directory, manifest, records)
        with generated_provider_scope(request) as current:
            assert (
                tuple(
                    storage.replay_broker_lifecycle(
                        directory, provider_request=request
                    )
                )
                == records
            )
            assert current.reads > 0
    _write(directory, a, records_a)
    return {
        "values": values,
        "a": a,
        "b": b,
        "records_a": records_a,
        "records_b": records_b,
        "directory": directory,
        "request": request,
        "metadata_a": identity_a.metadata.metadata.to_json(),
        "metadata_b": metadata_b.to_json(),
    }


def _structural_proof_a(journals):
    values = journals["values"]
    health, observations, manifest, _, invocation, pm, pc, pd, provider = values
    audit = replay_lifecycle_host_health(*values)
    permission = BrokerPermissionExecutionV1(
        pm,
        pd.binding,
        pc,
        pd,
        _chunks(invocation.to_json()),
        "lifecycle_manifest",
        _chunks(manifest.to_json()),
        manifest.artifact_id,
        _sha(manifest),
    )
    environment = json.loads(host_environment_json())
    environment["schema_version"] = (
        "histdatacom.broker-provenance-environment.v2"
    )
    environment["sdk_compatibility"] = make_capture_compatibility_declaration(
        invocation,
        manifest.header,
        host_version=environment["host_version"],
        started_at_utc_ns=health.started_at_utc_ns,
    ).to_dict()
    environment = canonical_native_json(environment)
    header = make_native_provenance_header(
        invocation,
        manifest.header,
        health,
        provider,
        environment_json=environment,
        permission_manifest=pm,
        permission_context=pc,
        permission_decision=pd,
    )
    proof = verified(
        generated_chain(
            header,
            terminal_changes={
                "stopped_at_utc_ns": observations[-1].utc_ns,
                "stopped_at_monotonic_ns": observations[-1].monotonic_ns,
                "native_manifest_id": manifest.artifact_id,
                "native_manifest_sha256": _sha(manifest),
                "health_audit_id": audit.artifact_id,
                "health_audit_sha256": _sha(audit),
                "permission_execution_id": permission.artifact_id,
                "permission_execution_sha256": _sha(permission),
            },
        )
    )
    assert proof.complete and proof.anchored
    assert proof.seal.terminal.native_manifest_id == manifest.artifact_id
    assert proof.seal.terminal.native_manifest_sha256 == _sha(manifest)
    return proof, environment, audit, permission


def test_both_coherent_sources_replay_without_expected_manifest(journals):
    # Fixture setup exercised both complete journals using the unchanged API.
    assert journals["a"].header == journals["b"].header


@pytest.mark.parametrize("which", ("a", "b"))
def test_matching_expected_manifest_replays_all_exact_records(journals, which):
    manifest, records = journals[which], journals["records_" + which]
    _write(journals["directory"], manifest, records)
    with generated_provider_scope(journals["request"]):
        assert (
            tuple(
                storage.replay_broker_lifecycle(
                    journals["directory"],
                    provider_request=journals["request"],
                    expected_manifest=manifest,
                )
            )
            == records
        )


def test_different_coherent_manifest_refuses_before_first_record(journals):
    _write(journals["directory"], journals["b"], journals["records_b"])
    emitted = []
    with generated_provider_scope(journals["request"]):
        with pytest.raises(BrokerLifecycleError) as caught:
            emitted.extend(
                storage.replay_broker_lifecycle(
                    journals["directory"],
                    provider_request=journals["request"],
                    expected_manifest=journals["a"],
                )
            )
    assert caught.value.reason is BrokerLifecycleReason.INTEGRITY
    assert emitted == []


@pytest.mark.parametrize("kind", ("mapping", "subclass"))
def test_expected_manifest_requires_exact_type_before_io(
    journals, monkeypatch, kind
):
    class DerivedManifest(BrokerLifecycleManifestV1):
        pass

    expected = journals["a"].to_dict()
    if kind == "subclass":
        expected = DerivedManifest(
            **{
                field.name: getattr(journals["a"], field.name)
                for field in fields(journals["a"])
            }
        )

    def forbidden_inspection(*args, **kwargs):
        pytest.fail("invalid expected-manifest type reached source IO")

    monkeypatch.setattr(
        storage, "inspect_broker_lifecycle", forbidden_inspection
    )
    with pytest.raises(BrokerLifecycleError) as caught:
        tuple(
            storage.replay_broker_lifecycle(
                journals["directory"],
                provider_request=journals["request"],
                expected_manifest=expected,
            )
        )
    assert caught.value.reason is BrokerLifecycleReason.INTEGRITY


def test_original_verify_to_yield_pass_still_rehashes_fixed_receipts(
    journals, monkeypatch
):
    read = storage._read
    reads = 0

    def replace_after_verified_read(path, maximum):
        nonlocal reads
        if path.name == "partition-0000.jsonl":
            reads += 1
        result = read(path, maximum)
        if path.name == "partition-0000.jsonl" and reads == 1:
            _write(journals["directory"], journals["b"], journals["records_b"])
        return result

    monkeypatch.setattr(storage, "_read", replace_after_verified_read)
    emitted = []
    with generated_provider_scope(journals["request"]):
        with pytest.raises(BrokerLifecycleError) as caught:
            emitted.extend(
                storage.replay_broker_lifecycle(
                    journals["directory"],
                    provider_request=journals["request"],
                )
            )
    assert caught.value.reason is BrokerLifecycleReason.INTEGRITY
    assert reads == 2 and emitted == []


def test_expected_manifest_does_not_cache_current_rights(journals):
    with generated_provider_scope(journals["request"]) as source:
        records = storage.replay_broker_lifecycle(
            journals["directory"],
            provider_request=journals["request"],
            expected_manifest=journals["a"],
        )
        assert next(records) == journals["records_a"][0]
        before = source.reads
        current = source.current
        selected = next(
            p
            for p in current.policies
            if p.artifact_id == current.selected_policy_ids[0]
        )
        now = time.time_ns()
        revoked = BrokerPolicyRevocationV1(
            selected.artifact_id,
            now,
            now,
            selected.evidence_ids,
            "withdrawn",
        )
        source.current = replace(current, revocations=(revoked,))
        with pytest.raises(BrokerPolicyError) as caught:
            next(records)
        assert caught.value.reason == "operation_not_allowed"
        assert any(
            "policy_revoked" in cell.reasons
            for cell in caught.value.decision.cells
        )
        assert source.reads > before


@pytest.mark.parametrize("substitute", (False, True))
def test_compatibility_metadata_stays_bound_to_structural_proof_a(
    journals, monkeypatch, substitute
):
    proof, environment, _, _ = _structural_proof_a(journals)
    directory = journals["directory"]
    metadata_directory = directory.with_name(directory.name + "-provenance")
    metadata_directory.mkdir()
    (metadata_directory / "environment.json").write_text(
        environment, encoding="ascii"
    )
    calls = []

    def already_verified_a(path, manifest, *, provider_request, expected_root):
        assert path == directory and manifest == journals["a"]
        assert (
            provider_request == journals["request"]
            and expected_root == proof.seal
        )
        assert (
            tuple(
                storage.replay_broker_lifecycle(
                    path, provider_request=provider_request
                )
            )
            == journals["records_a"]
        )
        calls.append(manifest.artifact_id)
        if substitute:
            _write(directory, journals["b"], journals["records_b"])
        return proof

    monkeypatch.setattr(
        native, "read_lifecycle_capture_provenance", already_verified_a
    )
    with generated_provider_scope(journals["request"]):
        if substitute:
            with pytest.raises(BrokerLifecycleError) as caught:
                compatibility.read_lifecycle_capture_compatibility(
                    directory,
                    journals["a"],
                    provider_request=journals["request"],
                    expected_root=proof.seal,
                )
            assert caught.value.reason is BrokerLifecycleReason.INTEGRITY
        else:
            result = compatibility.read_lifecycle_capture_compatibility(
                directory,
                journals["a"],
                provider_request=journals["request"],
                expected_root=proof.seal,
            )
            assert result.runtime_metadata_jsons == (journals["metadata_a"],)
            assert result.provenance_seal_json == proof.seal.to_json()
    assert calls == [journals["a"].artifact_id]


def test_projection_cannot_consume_b_records_under_a_root(journals):
    proof, _, audit, _ = _structural_proof_a(journals)
    root_a = BrokerFingerprintLifecycleRootV1(
        journals["a"],
        journals["request"],
        audit,
        proof.header,
        proof.seal,
        None,
    )
    _write(journals["directory"], journals["b"], journals["records_b"])
    projected = projection._Projection(BrokerLifecycleFingerprintFitConfigV1())
    with generated_provider_scope(journals["request"]):
        with pytest.raises(BrokerLifecycleError) as caught:
            projected.consume(journals["directory"], root_a)
    assert caught.value.reason is BrokerLifecycleReason.INTEGRITY
    assert projected.records == 0 and projected.cells == {}


@pytest.mark.parametrize("substitute", (False, True))
def test_security_classification_uses_its_exact_native_manifest(
    journals, substitute
):
    from histdatacom.broker_plugin_lifecycle.supervisor import (
        BrokerLifecycleResultV1,
    )

    proof, _, audit, permission = _structural_proof_a(journals)
    directory = journals["directory"]
    result_a = BrokerLifecycleResultV1(
        directory,
        journals["a"],
        BrokerLifecycleReason.CLOSED,
        (),
        audit,
        directory.with_name(directory.name + "-host-health"),
        permission,
        proof.seal,
        directory.with_name(directory.name + "-provenance"),
    )
    policy = BrokerSecurityPolicyV1(
        journals["request"].plan.candidate.artifact_id,
        BrokerTrustTier.DEVELOPMENT,
        BrokerSecurityMode.KERNEL_ISOLATED,
    )
    if substitute:
        _write(directory, journals["b"], journals["records_b"])
    with generated_provider_scope(journals["request"]):
        if substitute:
            with pytest.raises(BrokerLifecycleError) as caught:
                security._verify_configuration_classification(
                    result_a, {}, {}, policy, journals["request"]
                )
            assert caught.value.reason is BrokerLifecycleReason.INTEGRITY
        else:
            assert (
                security._verify_configuration_classification(
                    result_a, {}, {}, policy, journals["request"]
                )
                is None
            )

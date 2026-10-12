"""Pure native scientific-admission canaries, not capture/replay qualification.

Existing generated SDK records and observations cross the real health reducer.
Fresh exact terminal identities bind each resulting manifest and audit. The
small provenance chain is structural test evidence only, not an acquired native
journal, an installed-plugin attestation, or a current provider-rights grant.
"""

import hashlib
import json

import pytest

from histdatacom.broker_capture import (
    BrokerLifecycleFingerprintFitConfigV1,
    assess_broker_capture_eligibility,
)
from histdatacom.broker_capture.fingerprint_v3 import (
    BrokerFingerprintLifecycleRootV1,
)
from histdatacom.broker_plugin_health import (
    BrokerHostHealthFailure,
    BrokerHostHealthState,
    replay_lifecycle_host_health,
)
from histdatacom.broker_plugin_lifecycle import BrokerLifecycleCompletion
from histdatacom.broker_plugin_permissions.provenance import (
    BrokerPermissionExecutionV1,
    _chunks,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceVerificationReason,
)
from histdatacom.broker_plugin_provenance.native_common import (
    host_environment_json,
)
from histdatacom.broker_plugin_provenance.runtime import (
    make_native_provenance_header,
)
from tests.fixtures.broker_provider_policy import generated_provider_scope
from tests.unit.test_broker_host_health_replay import sdk_fixture
from tests.unit.test_broker_provenance_core import generated_chain, verified


def _sha(artifact):
    return hashlib.sha256(artifact.to_json().encode("ascii")).hexdigest()


def _bound_inputs(mode="healthy"):
    values = list(
        sdk_fixture(
            source_order=(3, 1, 2) if mode == "degraded" else (1, 2, 3),
            gap=mode == "partial",
        )
    )
    if mode == "observations-incomplete":
        # Every persisted native record remains matched; only CLOSE is absent.
        values[1] = values[1][:-1]
    (
        health_header,
        observations,
        manifest,
        _,
        invocation,
        pm,
        pc,
        pd,
        provider,
    ) = values
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
    header = make_native_provenance_header(
        invocation,
        manifest.header,
        health_header,
        provider,
        environment_json=host_environment_json(),
        permission_manifest=pm,
        permission_context=pc,
        permission_decision=pd,
    )
    chain = generated_chain(
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
            "native_complete": (
                manifest.completion is BrokerLifecycleCompletion.COMPLETE
            ),
            "observations_complete": audit.complete_observations,
        },
    )
    seal = chain[-1]
    checked = verified(chain)
    assert checked.root_sha256 == seal.root_sha256
    assert checked.entry_count == seal.entry_count
    assert checked.checkpoint_count == seal.checkpoint_count
    assert checked.reason is (
        BrokerProvenanceVerificationReason.VERIFIED
        if seal.terminal.native_complete and seal.terminal.observations_complete
        else BrokerProvenanceVerificationReason.PARTIAL
    )
    # Round-trips prove these are valid typed envelopes, not malformed inputs.
    for value in (manifest, invocation, audit, header, seal, permission):
        assert type(value).from_json(value.to_json()) == value
    # Rebinding is explicit: no negative relies on an old manifest/audit hash.
    assert header.native_header_id == manifest.header.artifact_id
    assert header.native_header_sha256 == _sha(manifest.header)
    assert manifest.header.plan.to_json() == invocation.plan.to_json()
    assert audit.native_manifest_id == manifest.artifact_id
    assert seal.terminal.native_manifest_id == manifest.artifact_id
    assert seal.terminal.native_manifest_sha256 == _sha(manifest)
    assert seal.terminal.health_audit_id == audit.artifact_id
    assert seal.terminal.health_audit_sha256 == _sha(audit)
    return {
        "manifest": manifest,
        "invocation": invocation,
        "health_audit": audit,
        "header": header,
        "seal": seal,
        "compatibility_json": None,
    }


def test_exact_qualified_synthetic_native_root_is_positive_control():
    inputs = _bound_inputs()
    assert inputs["health_audit"].state is BrokerHostHealthState.QUALIFIED
    assert not inputs["health_audit"].failures
    root = BrokerFingerprintLifecycleRootV1(**inputs)
    assert (
        BrokerFingerprintLifecycleRootV1.from_dict(json.loads(root.to_json()))
        == root
    )


@pytest.mark.parametrize("mode", ("degraded", "observations-incomplete"))
def test_exact_native_root_refuses_independently_unqualified_health(mode):
    inputs = _bound_inputs(mode)
    assert inputs["manifest"].completion is BrokerLifecycleCompletion.COMPLETE
    audit = inputs["health_audit"]
    assert audit.state is not BrokerHostHealthState.QUALIFIED
    if mode == "degraded":
        assert audit.state is BrokerHostHealthState.DEGRADED
        assert audit.complete_observations
        assert BrokerHostHealthFailure.REORDER in audit.failures
    else:
        assert not audit.complete_observations
        assert BrokerHostHealthFailure.INCOMPLETE in audit.failures
    with pytest.raises(
        ValueError,
        match="^native root requires exact complete qualified evidence$",
    ):
        BrokerFingerprintLifecycleRootV1(**inputs)


def test_exact_native_root_refuses_coherent_partial_capture():
    inputs = _bound_inputs("partial")
    assert inputs["manifest"].completion is BrokerLifecycleCompletion.PARTIAL
    assert not inputs["seal"].terminal.native_complete
    assert BrokerHostHealthFailure.INCOMPLETE in inputs["health_audit"].failures
    with pytest.raises(
        ValueError,
        match="^native root requires exact complete qualified evidence$",
    ):
        BrokerFingerprintLifecycleRootV1(**inputs)


def test_partial_capture_refuses_public_eligibility_before_source_io(
    tmp_path, monkeypatch
):
    from histdatacom.broker_capture.lifecycle_fingerprints import _source_root
    from histdatacom.broker_plugin_lifecycle import storage

    inputs = _bound_inputs("partial")
    manifest, invocation = inputs["manifest"], inputs["invocation"]

    def unexpected_read(*args, **kwargs):
        pytest.fail("incomplete native input reached source IO")

    monkeypatch.setattr(storage, "inspect_broker_lifecycle", unexpected_read)
    directory = tmp_path / "not-created"
    with generated_provider_scope(invocation):
        with pytest.raises(
            ValueError,
            match="^native scientific fit requires a complete finite capture$",
        ):
            _source_root(
                directory,
                manifest,
                invocation,
                BrokerLifecycleFingerprintFitConfigV1(),
            )
        decision = assess_broker_capture_eligibility(
            directory, manifest, provider_request=invocation
        )
    assert not decision.eligible
    assert decision.reason_codes == (
        "native_source_integrity_health_or_provenance_unqualified",
    )
    assert not directory.exists()

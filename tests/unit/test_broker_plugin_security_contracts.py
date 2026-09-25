"""Portable policy/refusal/receipt tests, including unsupported execution."""

from __future__ import annotations

from dataclasses import replace
import json
import os
import secrets

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistrationV1,
)
from histdatacom.broker_plugin_security import (
    BrokerSecurityError,
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerSecurityReceiptV1,
    BrokerSoftwareProvenanceV1,
    BrokerTrustTier,
    canonical_security_json,
    read_security_receipt,
    write_security_receipt,
    run_secure_broker_plugin,
    run_trusted_broker_plugin,
)
from histdatacom.broker_plugin_security import isolation
from tests.fixtures.broker_provider_policy import (
    generated_provider_scope,
    generated_sdk_request,
)


def request():
    registration = BrokerPluginRegistrationV1(
        "org.example.portable",
        "1.0.0",
        "Portable fixture",
        "portable-fixture",
        "1.0.0",
        "portable_fixture.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("offline",),
        ("events.v1", "session.v1"),
    )
    candidate = BrokerPluginCandidateV1(registration, "a" * 64, "b" * 64)
    inventory = BrokerPluginInventoryV1((candidate,))
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(
            tuple(
                sorted(("configuration_schema", "open_session", "iter_events"))
            )
        ),
        plugin_id=registration.plugin_id,
    )
    policy = BrokerSecurityPolicyV1(
        candidate.artifact_id,
        BrokerTrustTier.DEVELOPMENT,
        BrokerSecurityMode.KERNEL_ISOLATED,
    )
    return inventory, plan, policy


def receipt():
    inventory, plan, policy = request()
    software = BrokerSoftwareProvenanceV1(
        policy.candidate_id,
        "portable-fixture",
        "1.0.0",
        inventory.sdk_version,
        plan.candidate.registration_sha256,
        plan.candidate.implementation_sha256,
    )
    return BrokerSecurityReceiptV1(
        policy,
        software,
        "broker-lifecycle-manifest:sha256:" + "c" * 64,
        "{}",
        "macos-seatbelt-v1",
    )


def test_symbol_workflow_is_preflighted_before_authorization_or_resolution(
    tmp_path,
):
    inventory, plan, policy = request()
    for mode in (
        BrokerSecurityMode.KERNEL_ISOLATED,
        BrokerSecurityMode.TRUSTED_IN_PROCESS,
    ):
        selected = replace(policy, trust=BrokerTrustTier.REVIEWED, mode=mode)
        arguments = (inventory, plan, selected, {}, ("EURUSD",))
        provider_request = generated_sdk_request(plan)
        keywords = {
            "authorize": lambda _: pytest.fail("must not authorize"),
            "provider_request": provider_request,
        }
        with generated_provider_scope(provider_request):
            with pytest.raises(BrokerSecurityError):
                if mode is BrokerSecurityMode.KERNEL_ISOLATED:
                    run_secure_broker_plugin(
                        *arguments, tmp_path / "output", **keywords
                    )
                else:
                    run_trusted_broker_plugin(*arguments, **keywords)
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("field", ["archive_sha256", "attestation_sha256"])
def test_optional_sha256_fields_refuse_sha1_length_even_with_resealed_id(field):
    import hashlib

    software = receipt().software
    with pytest.raises(BrokerSecurityError):
        replace(software, **{field: "f" * 40})
    payload = software.to_dict()
    payload[field] = "f" * 40
    payload.pop("artifact_id")
    payload["artifact_id"] = (
        "broker-security-software:sha256:"
        + hashlib.sha256(
            canonical_security_json(payload).encode("ascii")
        ).hexdigest()
    )
    with pytest.raises(BrokerSecurityError):
        BrokerSoftwareProvenanceV1.from_dict(payload)


def test_receipt_refuses_classified_secret_key_even_with_resealed_id():
    import hashlib

    value = receipt()
    value = replace(
        value, policy=replace(value.policy, secret_fields=("credential",))
    )
    with pytest.raises(BrokerSecurityError):
        replace(value, public_configuration_json='{"credential":"private"}')
    payload = value.to_dict()
    payload["public_configuration_json"] = '{"credential":"private"}'
    payload.pop("artifact_id")
    payload["artifact_id"] = (
        "broker-security-receipt:sha256:"
        + hashlib.sha256(
            canonical_security_json(payload).encode("ascii")
        ).hexdigest()
    )
    with pytest.raises(BrokerSecurityError):
        BrokerSecurityReceiptV1.from_dict(payload)


@pytest.mark.parametrize("platform", ["win32", "linux", "unsupported"])
def test_unsupported_isolation_has_no_authorization_resolution_or_output(
    platform, monkeypatch, tmp_path
):
    inventory, plan, policy = request()
    monkeypatch.setattr(isolation.sys, "platform", platform)

    class ForbiddenProvider:
        def resolve(self, handle):
            pytest.fail("must not resolve")

    provider_request = generated_sdk_request(plan)
    with generated_provider_scope(provider_request):
        with pytest.raises(
            BrokerSecurityError, match="security_backend_unsupported"
        ):
            run_secure_broker_plugin(
                inventory,
                plan,
                policy,
                {},
                (),
                tmp_path / "run",
                authorize=lambda _: pytest.fail("must not authorize"),
                secret_provider=ForbiddenProvider(),
                provider_request=provider_request,
            )
    assert list(tmp_path.iterdir()) == []


def test_security_receipt_exact_roundtrip_no_clobber_and_mutation(
    tmp_path, monkeypatch
):
    from histdatacom.broker_plugin_security import storage as security_storage
    from histdatacom.broker_plugin_lifecycle import (
        BrokerLifecycleCompletion,
        BrokerLifecycleHeaderV1,
        BrokerLifecycleManifestV1,
        BrokerLifecyclePolicyV1,
        BrokerLifecycleState,
    )

    inventory, plan, _ = request()
    provider_request = generated_sdk_request(plan)
    manifest = BrokerLifecycleManifestV1(
        BrokerLifecycleHeaderV1(
            inventory, plan, BrokerLifecyclePolicyV1(), (), "e" * 32, "3.10.19"
        ),
        BrokerLifecycleState.DISCOVERED,
        BrokerLifecycleCompletion.OPEN,
    )
    # This is a generated OPEN metadata record, never a successful capture.
    value = replace(receipt(), native_manifest_id=manifest.artifact_id)
    path = tmp_path / "security.json"
    with generated_provider_scope(provider_request):
        write_security_receipt(
            value, path, provider_request=provider_request, manifest=manifest
        )
        assert read_security_receipt(path) == value
        sidecar = path.with_name(path.name + ".provider-policy.json")
        original_sidecar = sidecar.read_bytes()
        write_security_receipt(
            value, path, provider_request=provider_request, manifest=manifest
        )
        assert sidecar.read_bytes() == original_sidecar
        original_native = path.read_bytes()
        with monkeypatch.context() as smaller_budget:
            smaller_budget.setattr(
                security_storage,
                "MAX_SECURITY_BYTES",
                len(original_sidecar) + len(original_native) - 1,
            )
            with pytest.raises(BrokerSecurityError):
                write_security_receipt(
                    value,
                    path,
                    provider_request=provider_request,
                    manifest=manifest,
                )
        assert path.read_bytes() == original_native
        assert sidecar.read_bytes() == original_sidecar
        for missing in (path, sidecar):
            retained = missing.read_bytes()
            missing.unlink()
            with pytest.raises(BrokerSecurityError):
                write_security_receipt(
                    value,
                    path,
                    provider_request=provider_request,
                    manifest=manifest,
                )
            assert not missing.exists()
            missing.write_bytes(retained)
        sidecar.write_bytes(original_sidecar + b" ")
        with pytest.raises(BrokerSecurityError):
            write_security_receipt(
                value,
                path,
                provider_request=provider_request,
                manifest=manifest,
            )
        assert path.read_bytes() == original_native
        assert sidecar.read_bytes() == original_sidecar + b" "
        sidecar.write_bytes(original_sidecar)
        altered = replace(
            value,
            native_manifest_id="broker-lifecycle-manifest:sha256:" + "d" * 64,
        )
        with pytest.raises(ValueError, match="native manifest differs"):
            write_security_receipt(
                altered,
                path,
                provider_request=provider_request,
                manifest=manifest,
            )
    assert read_security_receipt(path) == value
    path.write_text(
        value.to_json().replace("portable-fixture", "other-fixture")
    )
    with pytest.raises(BrokerSecurityError):
        read_security_receipt(path)


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory"])
def test_nonregular_receipt_refused_without_blocking(tmp_path, kind):
    if kind != "directory" and os.name != "posix":
        pytest.skip("POSIX filesystem fixture")
    path = tmp_path / "refused"
    if kind == "symlink":
        target = tmp_path / "target"
        target.write_text(receipt().to_json())
        path.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.mkdir()
    with pytest.raises(BrokerSecurityError):
        read_security_receipt(path)


def test_unknown_duplicate_fields_huge_scalars_and_alias_graphs_refused():
    value = receipt()
    with pytest.raises(BrokerSecurityError):
        BrokerSecurityReceiptV1.from_dict(
            {**value.to_dict(), "private_account": secrets.token_hex(8)}
        )
    with pytest.raises(BrokerSecurityError):
        BrokerSecurityReceiptV1.from_json(
            value.to_json().replace('{"', '{"unknown":1,"unknown":2,"', 1)
        )
    with pytest.raises(BrokerSecurityError):
        replace(value, publication_api_allowed=True)
    with pytest.raises(BrokerSecurityError):
        canonical_security_json({"value": 2**10_000})
    cycle = []
    cycle.append(cycle)
    with pytest.raises(BrokerSecurityError):
        canonical_security_json(cycle)
    alias = ["x" * 1024]
    for _ in range(6):
        alias = [alias] * 128
    with pytest.raises(BrokerSecurityError):
        canonical_security_json(alias)
    assert json.loads(value.to_json())["scientific_admission"] is False

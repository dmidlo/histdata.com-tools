"""Pure generated unit tests. Native/installed calls are explicitly stubbed.

These tests do not execute a plugin, start a worker, capture, or certify a kit.
They use public contracts, not repository fixtures or implementation imports.
"""

import hashlib
import importlib.util
import json
import sys
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from histdatacom.broker_plugin_capabilities import validate_broker_capability_event
from histdatacom.broker_plugin_conformance import (
    BrokerConformanceDriverV1,
    BrokerConformanceScenarioV1,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleHeaderV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePartitionV1,
    BrokerLifecycleRecordV1,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionCacheV1,
    BrokerPermissionContextV1,
    BrokerPermissionEndpointV1,
    BrokerPermissionError,
    BrokerPermissionManifestV1,
    BrokerPermissionRevocationV1,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyDataClass,
    BrokerPolicyError,
    BrokerPolicyOperation,
    BrokerPolicyRequestV1,
    BrokerPolicyRetention,
    BrokerPolicyRevocationV1,
    BrokerPolicyStatus,
    BrokerProviderOutputContractV1,
    BrokerSDKInvocationV1,
    BrokerSDKRecordV1,
    decide_provider_operation,
    resolve_provider_subject,
)
from histdatacom.broker_plugin_provenance import BrokerProvenanceTerminalV1
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistrationV1,
)
from histdatacom.broker_plugin_security import BrokerSoftwareProvenanceV1
from histdatacom.broker_plugins import (
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerDiagnosticSeverity,
    BrokerDiagnosticV1,
    BrokerEventKind,
    BrokerEventV1,
    BrokerPluginMetadataV1,
    BrokerQuoteV1,
    BrokerReasonCode,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSourceTimeSemantics,
    BrokerSourceTimeV1,
)

SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "capture_example.py"
SPEC = importlib.util.spec_from_file_location("standalone_capture_example", SCRIPT)
example = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = example
SPEC.loader.exec_module(example)


@pytest.fixture
def inputs():
    registration = BrokerPluginRegistrationV1(
        "org.example.operator",
        "0.1.0",
        "Generated operator unit fixture",
        "generated-operator-test",
        "0.1.0",
        "generated_operator.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("generated",),
        example.CAPABILITIES,
    )
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration.to_json().encode("ascii")).hexdigest(),
        "a" * 64,
    )
    inventory = BrokerPluginInventoryV1((candidate,))
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        "generated-operator-test",
        "0.1.0",
        "1.0.0",
        ("generated",),
        ("emit:health", "emit:quotes"),
        ("emit:sizes", "raw_payload:emit"),
    )
    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "mode",
                BrokerConfigurationType.STRING,
                "Generated mode",
                choices=("finite",),
            ),
        )
    )
    driver = BrokerConformanceDriverV1(
        candidate.artifact_id,
        manifest.artifact_id,
        "generated",
        schema.to_json(),
        ("EURUSD",),
        (BrokerConformanceScenarioV1("finite", '{"mode":"finite"}'),),
    )
    return inventory, driver, manifest


def test_pure_request_is_exact_and_no_raw_or_private_scope(inputs):
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    example.validate_retained_scope(driver, manifest, request)
    assert request.output_contract == BrokerProviderOutputContractV1(
        "sdk-v1", ("health", "quote")
    )
    assert request.configuration_profile.private_field_names == ()
    assert "timestamps.receive.v1" in request.plan.enabled_optional
    assert "timestamps.receive.v1" not in request.plan.required


def test_pure_health_prelude_does_not_fabricate_receive_clock(inputs):
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    metadata = BrokerPluginMetadataV1("org.example.operator", "0.1.0", "Generated")
    session = BrokerSessionV1(metadata.artifact_id, "a" * 32, 1, "clock")
    event = BrokerEventV1(
        session.artifact_id,
        0,
        BrokerEventKind.HEALTH,
        "connection",
        diagnostic=BrokerDiagnosticV1(
            BrokerReasonCode.HEALTHY,
            BrokerDiagnosticSeverity.INFO,
            BrokerReasonCode.HEALTHY.value,
        ),
    )
    admitted = validate_broker_capability_event(request.plan, event)
    assert admitted.event.receive_time is None


@pytest.mark.parametrize(
    "change", ("network", "secret", "cache", "subprocess", "required-raw")
)
def test_pure_resource_declarations_refuse_even_if_optional(inputs, change):
    inventory, driver, manifest = inputs
    if change == "network":
        manifest = replace(
            manifest,
            optional_atoms=tuple(
                sorted((*manifest.optional_atoms, "network:provider:generated"))
            ),
            endpoints=(
                BrokerPermissionEndpointV1(
                    "feed", "generated", "http://127.0.0.1", "/", ("GET",), 0, 128, 100
                ),
            ),
        )
    elif change == "secret":
        manifest = replace(
            manifest,
            optional_atoms=tuple(
                sorted(
                    (
                        *manifest.optional_atoms,
                        "network:provider:generated",
                        "secrets:read:profile",
                    )
                )
            ),
            secret_profiles=("profile",),
            endpoints=(
                BrokerPermissionEndpointV1(
                    "feed",
                    "generated",
                    "http://127.0.0.1",
                    "/",
                    ("GET",),
                    0,
                    128,
                    100,
                    ("profile",),
                ),
            ),
        )
    elif change == "cache":
        manifest = replace(
            manifest,
            optional_atoms=tuple(
                sorted((*manifest.optional_atoms, "cache:plugin:fixture"))
            ),
            caches=(BrokerPermissionCacheV1("fixture", 128, 1, 128),),
        )
    elif change == "subprocess":
        manifest = replace(
            manifest,
            optional_atoms=tuple(
                sorted((*manifest.optional_atoms, "subprocess:requested"))
            ),
            subprocess_mode="isolated",
        )
    else:
        manifest = replace(
            manifest,
            required_atoms=tuple(
                sorted((*manifest.required_atoms, "raw_payload:emit"))
            ),
            optional_atoms=("emit:sizes",),
        )
    driver = replace(driver, permission_manifest_id=manifest.artifact_id)
    with pytest.raises(example.ExampleRefusal, match="emission_only"):
        example.make_request(inventory, driver, manifest)


def test_pure_mismatched_manifest_and_candidate_refuse(inputs):
    inventory, driver, manifest = inputs
    driver = replace(driver, candidate_id="broker-plugin-candidate:sha256:" + "b" * 64)
    with pytest.raises(example.ExampleRefusal, match="exact_installed_candidate"):
        example.make_request(inventory, driver, manifest)


@pytest.mark.parametrize(
    "configuration", ('{"mode":"reconnect"}', '{"mode":"finite","secret":"never"}')
)
def test_pure_nonfinite_or_extra_configuration_refuses(inputs, configuration):
    _, driver, _ = inputs
    # Constructing the public driver can itself reject an unknown schema key.
    with pytest.raises(ValueError):
        changed = replace(
            driver, scenarios=(BrokerConformanceScenarioV1("finite", configuration),)
        )
        example.finite_configuration(changed)


def test_pure_secret_schema_refuses(inputs):
    _, driver, _ = inputs
    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "mode", BrokerConfigurationType.STRING, "Private", secret=True
            ),
        )
    )
    with pytest.raises(ValueError):
        example.finite_configuration(
            replace(driver, configuration_schema_json=schema.to_json())
        )


def test_pure_policy_has_exact49_cells_and_denies_credentials_publish_derivation(
    inputs,
):
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    context = example.policy_context(
        request, replay_only=False, now_ns=1_700_000_000_000_000_000
    )
    policy = context.policies[0]
    assert len(policy.rules) == 49
    assert len({(r.operation, r.data_class) for r in policy.rules}) == 49
    for rule in policy.rules:
        allowed = rule.operation in {
            BrokerPolicyOperation.INVOKE,
            BrokerPolicyOperation.CAPTURE,
            BrokerPolicyOperation.MATERIAL_USE,
            BrokerPolicyOperation.RETAIN_LOCAL,
        } and rule.data_class in {
            BrokerPolicyDataClass.HEALTH,
            BrokerPolicyDataClass.NORMALIZED_QUOTES,
            BrokerPolicyDataClass.CONTENT_HASHES,
        }
        assert (rule.status is BrokerPolicyStatus.ALLOWED) == allowed
        if allowed and rule.operation is BrokerPolicyOperation.RETAIN_LOCAL:
            assert rule.retention is BrokerPolicyRetention.UNBOUNDED
    assert context.acknowledgements[0].policy_id == policy.artifact_id


def test_pure_replay_policy_is_fresh_and_material_use_only(inputs):
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    before = example.policy_context(
        request, replay_only=True, now_ns=1_700_000_000_000_000_000
    )
    after = example.policy_context(
        request, replay_only=True, now_ns=1_700_000_001_000_000_000
    )
    assert before.artifact_id != after.artifact_id
    assert {
        r.operation
        for r in after.policies[0].rules
        if r.status is BrokerPolicyStatus.ALLOWED
    } == {BrokerPolicyOperation.MATERIAL_USE}


def test_pure_permission_grants_only_needed_emissions_and_real_clock(inputs):
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    authority = example.permission_authority(request, manifest, now_ns=time.time_ns())
    decision = authority.require_admission()
    assert decision.effective_atoms == ("emit:health", "emit:quotes")
    assert decision.denied_optional_atoms == ("emit:sizes", "raw_payload:emit")
    assert (
        decision.binding.configuration_id == request.configuration_profile.artifact_id
    )


def test_pure_sources_reread_current_context(inputs, monkeypatch):
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    policy = example.PolicySource(
        example.policy_context(
            request, replay_only=True, now_ns=1_700_000_000_000_000_000
        )
    )
    previous = policy.read_policy_context()
    policy.context = example.policy_context(
        request, replay_only=True, now_ns=1_700_000_001_000_000_000
    )
    assert policy.read_policy_context() != previous
    source_type = example.PermissionSource
    sources = []

    def capture_source(context):
        source = source_type(context)
        sources.append(source)
        return source

    monkeypatch.setattr(example, "PermissionSource", capture_source)
    authority = example.permission_authority(request, manifest, now_ns=time.time_ns())
    assert authority.require_admission().admitted
    context = authority.read_context()
    grant = context.grants[0]
    source = sources[0]
    source.context = BrokerPermissionContextV1(
        context.grants,
        (
            BrokerPermissionRevocationV1(
                grant.artifact_id, grant.issued_at_ns, "operator-revoked"
            ),
        ),
        1,
    )
    assert source.read_context().revocations
    with pytest.raises(BrokerPermissionError) as refused:
        authority.require_admission()
    assert refused.value.decision.reason.value == "permission_grant_revoked"


def test_pure_retained_raw_output_scope_refuses(inputs):
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    changed = BrokerSDKInvocationV1(
        request.plan,
        request.configuration_profile,
        BrokerProviderOutputContractV1(
            "sdk-v1", ("health", "quote"), allow_opaque_metadata=True
        ),
    )
    with pytest.raises(example.ExampleRefusal, match="retained_generated_scope"):
        example.validate_retained_scope(driver, manifest, changed)


def test_io_create_once_never_replaces_and_leaf_symlinks_refuse(tmp_path):
    path = tmp_path / "artifact.json"
    example.create_once(path, "{}")
    with pytest.raises(FileExistsError):
        example.create_once(path, '{"changed":true}')
    alias = tmp_path / "alias.json"
    alias.symlink_to(path)
    with pytest.raises(OSError):
        example.read_text(alias)
    with pytest.raises(FileExistsError):
        example.create_once(alias, "{}")
    assert example.read_text(path) == "{}"


def test_io_bounded_reads_and_writes(tmp_path, monkeypatch):
    monkeypatch.setattr(example, "MAX_ANCHOR_BYTES", 2)
    with pytest.raises(example.ExampleRefusal, match="bounded_artifact"):
        example.create_once(tmp_path / "large.json", "{} ")
    (tmp_path / "large.json").write_text("{} ")
    with pytest.raises(example.ExampleRefusal, match="bounded_regular"):
        example.read_text(tmp_path / "large.json")


@pytest.mark.parametrize(
    "suffix", ("", "-host-health", "-provenance", "-security.json")
)
def test_io_anchor_is_outside_all_native_artifacts(tmp_path, suffix):
    with pytest.raises(example.ExampleRefusal, match="independent_anchor"):
        example.output_paths(
            tmp_path / "native", tmp_path / ("native" + suffix), new=True
        )


def test_cli_missing_authorization_has_no_effect(tmp_path, monkeypatch):
    monkeypatch.setattr(
        example, "discover_broker_plugins", lambda: pytest.fail("must not discover")
    )
    with pytest.raises(SystemExit) as stopped:
        example.main(
            [
                "capture",
                "--driver",
                "driver.json",
                "--native-dir",
                str(tmp_path / "n"),
                "--anchor-dir",
                str(tmp_path / "a"),
            ]
        )
    assert stopped.value.code == 2
    assert not tuple(tmp_path.iterdir())


@pytest.mark.parametrize("resource_abi", ("none", "host_resources_v1"))
def test_pure_capture_stub_preserves_partial_anchor_and_scrubs_cli_error(
    inputs, tmp_path, monkeypatch, capsys, resource_abi
):
    """Native execution is a failing stub: this is NOT a physical capture test."""
    inventory, driver, manifest = inputs
    manifest = replace(manifest, resource_abi=resource_abi)
    driver = replace(driver, permission_manifest_id=manifest.artifact_id)
    driver_file = tmp_path / "driver.json"
    example.create_once(driver_file, driver.to_json())
    monkeypatch.setattr(example, "discover_broker_plugins", lambda: inventory)
    monkeypatch.setattr(example, "inspect_broker_conformance_driver", lambda *_: driver)
    monkeypatch.setattr(
        example, "read_installed_broker_permissions", lambda _: manifest
    )
    called = []

    def native_stub(*args, **kwargs):
        called.append(True)
        assert kwargs["authorize"](args[2]) is True
        assert kwargs["authorize"](args[1]) is False
        assert (
            kwargs["authorize"](
                replace(
                    args[2], candidate_id="broker-plugin-candidate:sha256:" + "f" * 64
                )
            )
            is False
        )
        assert (
            kwargs["authorize"](
                replace(args[2], trust=example.BrokerTrustTier.REVIEWED)
            )
            is False
        )
        assert kwargs["lifecycle_policy"].run_timeout_ms == 120_000
        assert "clock" not in kwargs
        assert "execution_hooks" not in kwargs
        assert args[2].network is example.BrokerNetworkMode.OFF
        raise RuntimeError("PRIVATE-TEST-ONLY-DO-NOT-LOG")

    monkeypatch.setattr(example, "run_secure_broker_plugin", native_stub)
    anchor = tmp_path / "anchor"
    code = example.main(
        [
            "capture",
            "--driver",
            str(driver_file),
            "--native-dir",
            str(tmp_path / "native"),
            "--anchor-dir",
            str(anchor),
            "--authorize-generated-execution",
        ]
    )
    assert code == 2 and called == [True]
    assert "PRIVATE-TEST-ONLY" not in capsys.readouterr().err
    assert {p.name for p in anchor.iterdir()} == {
        "driver.json",
        "permissions.json",
        "request.json",
        "capture-outcome.json",
    }
    assert (
        json.loads(example.read_text(anchor / "capture-outcome.json"))["returned"]
        is False
    )
    with pytest.raises(example.ExampleRefusal, match="complete_external_anchor"):
        example.replay(tmp_path / "native", anchor, authorized=True)


def test_pure_replay_uses_retained_request_without_discovery_or_grant(
    inputs, tmp_path, monkeypatch
):
    """Seal/native readers are stubs; this proves orchestration, NOT integrity."""
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    anchor = tmp_path / "anchor"
    anchor.mkdir()
    for name, text in (
        ("driver.json", driver.to_json()),
        ("permissions.json", manifest.to_json()),
        ("request.json", request.to_json()),
        ("expected-seal.json", "{}"),
        ("expected-security.json", "{}"),
        ("capture-outcome.json", "{}"),
    ):
        example.create_once(anchor / name, text)
    sentinel = object()
    monkeypatch.setattr(
        example, "BrokerProvenanceSealV1", SimpleNamespace(from_json=lambda _: sentinel)
    )
    security_sentinel = object()
    monkeypatch.setattr(
        example,
        "BrokerSecurityReceiptV1",
        SimpleNamespace(from_json=lambda _: security_sentinel),
    )
    monkeypatch.setattr(
        example,
        "discover_broker_plugins",
        lambda: pytest.fail("no installed discovery on replay"),
    )
    monkeypatch.setattr(
        example,
        "permission_authority",
        lambda *_a, **_k: pytest.fail("no new plugin grant on replay"),
    )

    def native_stub(native, retained, seal, expected_security):
        assert (
            retained == request
            and seal is sentinel
            and expected_security is security_sentinel
        )
        return {"status": "stubbed_native_reader_only"}

    monkeypatch.setattr(example, "verify_finite_native", native_stub)
    assert (
        example.replay(tmp_path / "native", anchor, authorized=True)["status"]
        == "stubbed_native_reader_only"
    )


def test_pure_replay_rejects_partial_native_before_reading_partitions(
    tmp_path, monkeypatch
):
    """Inspection is an explicit stub, not a real native manifest."""
    monkeypatch.setattr(
        example,
        "inspect_broker_lifecycle",
        lambda _: SimpleNamespace(complete=False, manifest=object()),
    )
    monkeypatch.setattr(
        example,
        "replay_broker_lifecycle",
        lambda *_a, **_k: pytest.fail("partial inspection must refuse"),
    )
    with pytest.raises(example.ExampleRefusal, match="complete_native"):
        example.verify_finite_native(tmp_path / "native", object(), object(), object())


@pytest.mark.parametrize("command", ("capture", "replay"))
def test_pure_authorization_refuses_before_io_and_discovery(
    tmp_path, monkeypatch, command
):
    monkeypatch.setattr(
        example, "output_paths", lambda *_a, **_k: pytest.fail("must not inspect paths")
    )
    monkeypatch.setattr(
        example, "discover_broker_plugins", lambda: pytest.fail("must not discover")
    )
    with pytest.raises(example.ExampleRefusal, match="authorization_required"):
        if command == "capture":
            example.capture(
                tmp_path / "d", tmp_path / "n", tmp_path / "a", authorized=False
            )
        else:
            example.replay(tmp_path / "n", tmp_path / "a", authorized=False)


def test_io_sibling_outputs_allowed_but_nested_both_directions_refuse(tmp_path):
    native, anchor = example.output_paths(
        tmp_path / "native", tmp_path / "anchor", new=True
    )
    assert native.parent == anchor.parent
    native.mkdir()
    anchor.mkdir()
    for left, right in ((native, native / "anchor"), (anchor / "native", anchor)):
        with pytest.raises(example.ExampleRefusal, match="independent_anchor"):
            example.output_paths(left, right, new=True)


def test_io_existing_or_symlink_output_leaf_is_never_adopted(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    for path in (target, alias):
        with pytest.raises(example.ExampleRefusal, match="new_output_paths"):
            example.output_paths(path, tmp_path / "anchor", new=True)
        with pytest.raises(example.ExampleRefusal, match="new_output_paths"):
            example.output_paths(tmp_path / "native", path, new=True)


def test_pure_symbols_refuse_before_native_run(inputs):
    _, driver, _ = inputs
    # Deliberately corrupt an already constructed frozen public contract. Normal
    # from_json also refuses this; this test exercises the local early guard.
    object.__setattr__(driver, "symbols", ("GBPUSD",))
    with pytest.raises(example.ExampleRefusal, match="exact_generated_symbol"):
        example.finite_configuration(driver)


def test_pure_retained_driver_permission_identity_mismatch_refuses(inputs):
    inventory, driver, manifest = inputs
    request = example.make_request(inventory, driver, manifest)
    changed = replace(
        driver, permission_manifest_id="broker-permission-manifest:sha256:" + "b" * 64
    )
    with pytest.raises(example.ExampleRefusal, match="exact_candidate_permission"):
        example.validate_retained_scope(changed, manifest, request)


def native_reader_stubs(
    inputs,
    monkeypatch,
    native_path,
    *,
    unhealthy=False,
    wrong_seal=False,
    changed_security=None,
    reader_failure=None,
    observation=None,
    prepared=None,
):
    """Pure wrapper fixture. Every filesystem-native reader below is stubbed."""
    inventory, driver, permissions = inputs
    request = example.make_request(inventory, driver, permissions)
    metadata = BrokerPluginMetadataV1("org.example.operator", "0.1.0", "Generated")
    session = BrokerSessionV1(metadata.artifact_id, "a" * 32, 1, "clock")
    diagnostic = BrokerDiagnosticV1(
        BrokerReasonCode.HEALTHY,
        (
            BrokerDiagnosticSeverity.WARNING
            if unhealthy
            else BrokerDiagnosticSeverity.INFO
        ),
        BrokerReasonCode.HEALTHY.value,
    )
    events = [
        BrokerEventV1(
            session.artifact_id,
            0,
            BrokerEventKind.HEALTH,
            "connection",
            diagnostic=diagnostic,
        )
    ]
    for number in range(1, 4):
        events.append(
            BrokerEventV1(
                session.artifact_id,
                number,
                BrokerEventKind.QUOTE,
                "connection",
                source_time=BrokerSourceTimeV1(
                    number * 1_000_000_000, 1, BrokerSourceTimeSemantics.BROKER_EVENT
                ),
                receive_time=BrokerReceiveTimeV1(10 + number, 10 + number, "clock"),
                instrument="EURUSD",
                quote=BrokerQuoteV1("EURUSD", "1.10000", "1.20000"),
            )
        )
    transitions = (
        example.BrokerLifecycleTransitionV1(
            example.BrokerLifecycleState.ACTIVE,
            example.BrokerLifecycleState.STOPPING,
            example.BrokerLifecycleReason.EOF,
            0,
        ),
        example.BrokerLifecycleTransitionV1(
            example.BrokerLifecycleState.STOPPING,
            example.BrokerLifecycleState.STOPPED,
            example.BrokerLifecycleReason.CLOSED,
            0,
        ),
    )
    header = BrokerLifecycleHeaderV1(
        inventory,
        request.plan,
        example.BrokerLifecyclePolicyV1(partition_events=3),
        ("EURUSD",),
        "b" * 32,
        "3.10.19",
    )
    payloads = tuple(
        ("event", validate_broker_capability_event(request.plan, event).to_json())
        for event in events
    ) + tuple(("transition", item.to_json()) for item in transitions)
    records = tuple(
        BrokerLifecycleRecordV1(
            header.artifact_id,
            index,
            0,
            100 + index,
            200 + index,
            kind,
            payload,
            index if kind == "event" else None,
        )
        for index, (kind, payload) in enumerate(payloads)
    )
    native = manifest_for_records(header, records)
    native_json = native.to_json()
    if prepared is not None:
        prepared.update(manifest=native, records=records)
    seal = SimpleNamespace(artifact_id="stub-expected-seal")
    candidate = request.plan.candidate
    receipt = example.BrokerSecurityReceiptV1(
        example.BrokerSecurityPolicyV1(
            candidate.artifact_id,
            example.BrokerTrustTier.DEVELOPMENT,
            example.BrokerSecurityMode.KERNEL_ISOLATED,
            example.BrokerNetworkMode.OFF,
        ),
        BrokerSoftwareProvenanceV1(
            candidate.artifact_id,
            candidate.registration.distribution_name,
            candidate.registration.distribution_version,
            "1.0.0",
            candidate.registration_sha256,
            candidate.implementation_sha256,
        ),
        native.artifact_id,
        request.configuration_profile.public_configuration_json,
        "macos-seatbelt-v1",
    )
    expected_security = receipt
    if changed_security == "trust":
        receipt = replace(
            receipt,
            policy=replace(receipt.policy, trust=example.BrokerTrustTier.REVIEWED),
        )
    elif changed_security == "configuration":
        receipt = replace(receipt, public_configuration_json='{"mode":"reconnect"}')

    def inspect_stub(location):
        assert location == native_path
        return SimpleNamespace(complete=True, manifest=native)

    def replay_stub(location, *, provider_request):
        assert location == native_path and provider_request is request
        return iter(records) if observation is None else observation(native, records)

    def read_security_stub(location):
        assert location == native_path.with_name(native_path.name + "-security.json")
        return receipt

    def verify_security_stub(actual_receipt, actual_manifest):
        assert actual_receipt is receipt and actual_manifest.to_json() == native_json
        if reader_failure == "security":
            raise ValueError("stubbed_security_integrity_failure")

    def current_rights_stub(subject, operation):
        assert type(subject) is example.BrokerSDKSecurityV1
        assert (
            subject.invocation is request
            and subject.receipt is receipt
            and subject.manifest.to_json() == native_json
        )
        assert operation is BrokerPolicyOperation.MATERIAL_USE

    monkeypatch.setattr(example, "inspect_broker_lifecycle", inspect_stub)
    monkeypatch.setattr(example, "replay_broker_lifecycle", replay_stub)
    monkeypatch.setattr(example, "read_security_receipt", read_security_stub)
    monkeypatch.setattr(example, "verify_security_capture", verify_security_stub)
    monkeypatch.setattr(example, "require_provider_operation", current_rights_stub)
    seen = []

    def provenance_stub(location, actual_manifest, *, provider_request, expected_root):
        assert location == native_path and actual_manifest.to_json() == native_json
        assert provider_request is request and expected_root is seal
        seen.append(expected_root)
        if reader_failure == "provenance":
            raise ValueError("stubbed_provenance_integrity_failure")
        return SimpleNamespace(
            reason=example.BrokerProvenanceVerificationReason.VERIFIED,
            anchored=True,
            complete=True,
            seal=object() if wrong_seal else seal,
        )

    monkeypatch.setattr(example, "read_lifecycle_capture_provenance", provenance_stub)
    return request, seal, expected_security, seen


def manifest_for_records(header, records):
    """Construct exact bounded public artifacts; no native filesystem is read."""
    partitions = []
    width = header.policy.partition_events
    for start in range(0, len(records), width):
        partition = records[start : start + width]
        encoded = "".join(record.to_json() + "\n" for record in partition).encode(
            "ascii"
        )
        partitions.append(
            BrokerLifecyclePartitionV1(
                len(partitions),
                hashlib.sha256(encoded).hexdigest(),
                len(encoded),
                start,
                len(partition),
                sum(record.kind == "event" for record in partition),
            )
        )
    events = sum(record.kind == "event" for record in records)
    return BrokerLifecycleManifestV1(
        header,
        example.BrokerLifecycleState.STOPPED,
        example.BrokerLifecycleCompletion.COMPLETE,
        tuple(partitions),
        appended_records=len(records),
        appended_events=events,
        received_events=events,
        worker_reaped=True,
    )


@pytest.fixture
def native_snapshot(inputs, monkeypatch, tmp_path):
    prepared = {}
    native_reader_stubs(inputs, monkeypatch, tmp_path / "native", prepared=prepared)
    return prepared["manifest"], prepared["records"]


def test_pure_observation_binding_detaches_exact_multi_partition_snapshot(
    native_snapshot,
):
    manifest, records = native_snapshot
    bound = example.bind_native_observations(manifest, iter(records))
    assert len(manifest.partitions) == 2
    assert tuple(item.to_json() for item in bound) == tuple(
        item.to_json() for item in records
    )
    assert all(left is not right for left, right in zip(bound, records))


@pytest.mark.parametrize(
    "mutation",
    (
        "metadata",
        "payload",
        "delete",
        "insert",
        "duplicate",
        "sequence",
        "order",
        "run",
        "type",
    ),
)
def test_pure_native_wrapper_rejects_substituted_observations_before_later_reads(
    inputs, tmp_path, monkeypatch, mutation
):
    def observations(_manifest, records):
        changed = list(records)
        if mutation == "metadata":
            changed[1] = replace(records[1], receive_utc_ns=111)
        elif mutation == "payload":
            admitted = example.BrokerAdmittedEventV1.from_json(records[1].payload_json)
            changed_event = replace(
                admitted.event, quote=replace(admitted.event.quote, bid="1.10001")
            )
            changed[1] = replace(
                records[1],
                payload_json=replace(admitted, event=changed_event).to_json(),
            )
        elif mutation == "delete":
            changed.pop()
        elif mutation == "insert":
            changed.append(records[-1])
        elif mutation == "duplicate":
            changed.insert(1, records[0])
        elif mutation == "sequence":
            changed[1] = replace(records[1], capture_sequence=2)
        elif mutation == "order":
            changed[1], changed[2] = changed[2], changed[1]
        elif mutation == "run":
            changed[1] = replace(
                records[1], run_id="broker-lifecycle-header:sha256:" + "f" * 64
            )
        else:
            changed[1] = SimpleNamespace(**records[1].to_dict())
        return iter(changed)

    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", observation=observations
    )
    monkeypatch.setattr(
        example,
        "read_security_receipt",
        lambda *_: pytest.fail("binding must precede rereads"),
    )
    with pytest.raises(example.ExampleRefusal, match="native_observation"):
        example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert seen == []


def test_pure_native_wrapper_detaches_yields_before_generator_mutates_them(
    inputs, tmp_path, monkeypatch
):
    changed = []

    def observations(_manifest, records):
        yield records[0]
        # Frozen contracts are intentionally corrupted as an adversarial
        # generator stimulus after the caller has already observed this record.
        object.__setattr__(records[0], "epoch", 1)
        changed.append(records[0].epoch)
        yield from records[1:]

    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", observation=observations
    )
    result = example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert result["events"] == 4 and changed == [1] and seen == [seal]


def test_pure_native_wrapper_rejects_aba_observations_even_when_pin_is_restored(
    inputs, tmp_path, monkeypatch
):
    restored = []

    def observations(manifest, records):
        original = manifest.partitions
        changed = (replace(records[0], receive_utc_ns=101),) + records[1:]
        alternate = manifest_for_records(manifest.header, changed)
        object.__setattr__(manifest, "partitions", alternate.partitions)

        def stream():
            try:
                # Restore A before yielding B: all later independent readers
                # would see original A, but B still must not borrow A's pin.
                object.__setattr__(manifest, "partitions", original)
                restored.append(True)
                yield from changed
            finally:
                object.__setattr__(manifest, "partitions", original)

        return stream()

    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", observation=observations
    )
    with pytest.raises(example.ExampleRefusal, match="native_observation_partition"):
        example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert restored == [True] and seen == []


def test_pure_native_wrapper_freezes_opening_manifest_before_replay_call(
    inputs, tmp_path, monkeypatch
):
    def observations(manifest, records):
        changed = (replace(records[0], receive_utc_ns=101),) + records[1:]
        alternate = manifest_for_records(manifest.header, changed)
        # Mutate the original inspection object synchronously, before an
        # iterator is returned, and try to substitute matching B observations.
        object.__setattr__(manifest, "partitions", alternate.partitions)
        return iter(changed)

    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", observation=observations
    )
    with pytest.raises(example.ExampleRefusal, match="native_observation_partition"):
        example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert seen == []


@pytest.mark.parametrize(
    "mutation",
    (
        "hash",
        "bytes",
        "events",
        "total_events",
        "partition_records",
        "partition_bytes",
        "capture_bytes",
    ),
)
def test_pure_observation_binding_enforces_receipts_totals_and_limits(
    native_snapshot, mutation
):
    manifest, records = native_snapshot
    first, second = manifest.partitions
    if mutation in {"hash", "bytes", "events"}:
        updates = {
            "hash": {"sha256": "f" * 64},
            "bytes": {"byte_count": first.byte_count + 1},
            "events": {"event_count": first.event_count - 1},
        }[mutation]
        manifest = replace(manifest, partitions=(replace(first, **updates), second))
    elif mutation == "total_events":
        manifest = replace(manifest, appended_events=3, received_events=3)
    else:
        updates = {
            "partition_records": {"partition_events": 2},
            "partition_bytes": {"partition_bytes": 1024},
            "capture_bytes": {"partition_bytes": 1024, "max_capture_bytes": 1024},
        }[mutation]
        policy = replace(manifest.header.policy, **updates)
        manifest = replace(manifest, header=replace(manifest.header, policy=policy))
    with pytest.raises(example.ExampleRefusal, match="native_observation"):
        example.bind_native_observations(manifest, iter(records))


@pytest.mark.parametrize("stage", ("before", "during", "after"))
def test_pure_native_wrapper_never_swallows_replay_refusal(
    inputs, tmp_path, monkeypatch, stage
):
    def observations(_manifest, records):
        if stage == "before":
            raise ValueError("stubbed_current_rights_refusal")

        def stream():
            yield from records[: (1 if stage == "during" else len(records))]
            raise ValueError("stubbed_current_rights_refusal")

        return stream()

    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", observation=observations
    )
    with pytest.raises(ValueError, match="stubbed_current_rights_refusal"):
        example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert seen == []


def test_pure_native_wrapper_requires_healthy_info_prelude(
    inputs, tmp_path, monkeypatch
):
    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", unhealthy=True
    )
    with pytest.raises(example.ExampleRefusal, match="generated_healthy_info"):
        example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert seen == []


def test_pure_native_wrapper_pins_seal_and_refuses_mismatch(
    inputs, tmp_path, monkeypatch
):
    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", wrong_seal=True
    )
    with pytest.raises(example.ExampleRefusal, match="anchored_provenance"):
        example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert seen == [seal]


def test_pure_native_wrapper_positive_is_only_stubbed_orchestration(
    inputs, tmp_path, monkeypatch
):
    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native"
    )
    result = example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert result["events"] == 4 and seen == [seal]
    assert result["nonclaim"] == example.NONCLAIM


@pytest.mark.parametrize("changed_security", ("trust", "configuration"))
def test_pure_valid_security_substitution_refuses_with_same_native_seal(
    inputs, tmp_path, monkeypatch, changed_security
):
    request, seal, security, seen = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", changed_security=changed_security
    )
    with pytest.raises(example.ExampleRefusal, match="original_security_receipt"):
        example.verify_finite_native(tmp_path / "native", request, seal, security)
    assert seen == []


@pytest.mark.parametrize("reader_failure", ("security", "provenance"))
def test_pure_native_reader_integrity_failures_propagate(
    inputs, tmp_path, monkeypatch, reader_failure
):
    request, seal, security, _ = native_reader_stubs(
        inputs, monkeypatch, tmp_path / "native", reader_failure=reader_failure
    )
    with pytest.raises(ValueError, match="stubbed_.*_integrity_failure"):
        example.verify_finite_native(tmp_path / "native", request, seal, security)


def test_pure_actual_native_health_classification_preserves_no_raw_policy(inputs):
    """Uses the real public native resolver and decision, no classification stub."""
    inventory, driver, permissions = inputs
    request = example.make_request(inventory, driver, permissions)
    metadata = BrokerPluginMetadataV1("org.example.operator", "0.1.0", "Generated")
    session = BrokerSessionV1(metadata.artifact_id, "a" * 32, 1, "clock")
    event = BrokerEventV1(
        session.artifact_id,
        0,
        BrokerEventKind.HEALTH,
        "connection",
        diagnostic=BrokerDiagnosticV1(
            BrokerReasonCode.HEALTHY,
            BrokerDiagnosticSeverity.INFO,
            BrokerReasonCode.HEALTHY.value,
        ),
    )
    subject = resolve_provider_subject(
        BrokerSDKRecordV1(request, event, session, metadata)
    )
    assert set(subject.data_classes) == {
        BrokerPolicyDataClass.HEALTH,
        BrokerPolicyDataClass.CONTENT_HASHES,
    }
    now = 1_700_000_000_000_000_000
    context = example.policy_context(request, replay_only=False, now_ns=now)
    assert decide_provider_operation(
        context, BrokerPolicyRequestV1(subject, BrokerPolicyOperation.CAPTURE, now)
    ).allowed
    changed = replace(
        event,
        diagnostic=replace(event.diagnostic, summary="Generated advisory raw text"),
    )
    with pytest.raises(ValueError):
        resolve_provider_subject(BrokerSDKRecordV1(request, changed, session, metadata))
    # Even an explicitly widened output shape cannot get raw rights from the
    # example's 49-cell operator policy. This is a pure negative declaration.
    raw_request = BrokerSDKInvocationV1(
        request.plan,
        request.configuration_profile,
        BrokerProviderOutputContractV1(
            "sdk-v1", ("health", "quote"), allow_opaque_metadata=True
        ),
    )
    raw_subject = resolve_provider_subject(
        BrokerSDKRecordV1(raw_request, changed, session, metadata)
    )
    assert BrokerPolicyDataClass.RAW_PAYLOAD in raw_subject.data_classes
    denied = decide_provider_operation(
        example.policy_context(raw_request, replay_only=False, now_ns=now),
        BrokerPolicyRequestV1(raw_subject, BrokerPolicyOperation.CAPTURE, now),
    )
    assert not denied.allowed
    assert any(
        cell.data_class is BrokerPolicyDataClass.RAW_PAYLOAD
        and cell.status is BrokerPolicyStatus.DENIED
        for cell in denied.cells
    )


@pytest.mark.parametrize("revoke_on_return", (False, True))
def test_pure_returned_partial_retained_only_under_same_fresh_source(
    inputs, tmp_path, monkeypatch, revoke_on_return
):
    """Real public rights checks around a constructed result; no worker or capture."""
    inventory, driver, permissions = inputs
    driver_file = tmp_path / "driver.json"
    example.create_once(driver_file, driver.to_json())
    monkeypatch.setattr(example, "discover_broker_plugins", lambda: inventory)
    monkeypatch.setattr(example, "inspect_broker_conformance_driver", lambda *_: driver)
    monkeypatch.setattr(
        example, "read_installed_broker_permissions", lambda _: permissions
    )
    sources = []
    source_type = example.PolicySource

    def current_source(context):
        source = source_type(context)
        sources.append(source)
        return source

    monkeypatch.setattr(example, "PolicySource", current_source)
    returned = []

    def native_stub(
        actual_inventory,
        plan,
        security_policy,
        configuration,
        symbols,
        output,
        *,
        authorize,
        provider_request,
        protected_paths,
        lifecycle_policy,
    ):
        assert actual_inventory is inventory and authorize(security_policy)
        assert configuration == {"mode": "finite"} and symbols == ("EURUSD",)
        header = BrokerLifecycleHeaderV1(
            inventory, plan, lifecycle_policy, symbols, "a" * 32, "3.10.19"
        )
        manifest = BrokerLifecycleManifestV1(
            header,
            example.BrokerLifecycleState.FAILED,
            example.BrokerLifecycleCompletion.PARTIAL,
            worker_reaped=True,
            unknown_loss=True,
        )
        terminal = BrokerProvenanceTerminalV1(
            "broker-provenance-header:sha256:" + "a" * 64,
            2,
            2,
            manifest.artifact_id,
            hashlib.sha256(manifest.to_json().encode("ascii")).hexdigest(),
            "generated-test-health",
            "c" * 64,
            None,
            None,
            False,
            True,
        )
        seal = example.BrokerProvenanceSealV1(
            terminal.header_id, terminal, "d" * 64, 1, 1, "e" * 64, 0
        )
        candidate = plan.candidate
        security = example.BrokerSecurityReceiptV1(
            security_policy,
            BrokerSoftwareProvenanceV1(
                candidate.artifact_id,
                candidate.registration.distribution_name,
                candidate.registration.distribution_version,
                "1.0.0",
                candidate.registration_sha256,
                candidate.implementation_sha256,
            ),
            manifest.artifact_id,
            '{"mode":"finite"}',
            "macos-seatbelt-v1",
        )
        result = SimpleNamespace(
            native=SimpleNamespace(
                manifest=manifest,
                provenance=seal,
                reason=example.BrokerLifecycleReason.RUN_TIMEOUT,
            ),
            security=security,
        )
        returned.append(result)
        if revoke_on_return:
            context = sources[0].context
            policy = context.policies[0]
            now = time.time_ns()
            sources[0].context = replace(
                context,
                revocations=(
                    BrokerPolicyRevocationV1(
                        policy.artifact_id,
                        now,
                        now,
                        policy.evidence_ids,
                        "withdrawn",
                    ),
                ),
            )
        return result

    monkeypatch.setattr(example, "run_secure_broker_plugin", native_stub)

    def inspect_stub(location):
        assert location == tmp_path / "native"
        return SimpleNamespace(complete=False, manifest=returned[0].native.manifest)

    monkeypatch.setattr(example, "inspect_broker_lifecycle", inspect_stub)
    expected_error = BrokerPolicyError if revoke_on_return else example.ExampleRefusal
    with pytest.raises(expected_error):
        example.capture(
            driver_file, tmp_path / "native", tmp_path / "anchor", authorized=True
        )
    names = {item.name for item in (tmp_path / "anchor").iterdir()}
    if revoke_on_return:
        assert names == {"driver.json", "permissions.json", "request.json"}
    else:
        assert names == example.ANCHOR_FILES
        assert (
            example.read_text(tmp_path / "anchor" / "expected-seal.json")
            == returned[0].native.provenance.to_json()
        )
        assert (
            example.read_text(tmp_path / "anchor" / "expected-security.json")
            == returned[0].security.to_json()
        )
        assert (
            json.loads(example.read_text(tmp_path / "anchor" / "capture-outcome.json"))[
                "completion"
            ]
            == example.BrokerLifecycleCompletion.PARTIAL.value
        )

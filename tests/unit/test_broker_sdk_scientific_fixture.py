"""Bounded pure checks of independent installed acceptance fixture semantics."""

import hashlib
import json
import runpy
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.broker_plugin_capabilities import BrokerAdmittedEventV1
from histdatacom.broker_plugins import (
    BrokerEventKind,
)
from tests.fixtures.broker_sdk_scientific_qualification import (
    check_anchored_provenance,
    check_numeric,
    create_once,
    health_policy,
    installed_host,
    quote_oracle,
    read_record,
    recorded_conformance_refusal,
    require_integrity_refusal,
    require_policy_refusal,
    require_refusal,
)


@pytest.fixture
def generated_seal():
    from histdatacom.broker_plugin_provenance import (
        BrokerProvenanceSealV1,
        BrokerProvenanceTerminalV1,
    )

    header = "broker-provenance-header:sha256:" + "0" * 64
    terminal = BrokerProvenanceTerminalV1(
        header,
        300,
        400,
        "generated-manifest",
        "1" * 64,
        "generated-health-audit",
        "2" * 64,
        None,
        None,
        True,
        True,
    )
    return BrokerProvenanceSealV1(header, terminal, "3" * 64, 1, 1, "4" * 64, 0)


def test_record_frame_roundtrip_preserves_exact_canonical_artifact(
    tmp_path, generated_seal
):
    path = tmp_path / "seal.json"
    canonical = generated_seal.to_json()
    create_once(path, canonical)
    assert path.read_bytes() == canonical.encode("ascii") + b"\n"
    retained = read_record(path)
    assert retained == canonical
    restored = type(generated_seal).from_json(retained)
    assert restored == generated_seal and restored.to_json() == canonical
    with pytest.raises(ValueError, match="noncanonical"):
        type(generated_seal).from_json(path.read_text())


@pytest.mark.parametrize(
    "corruption",
    (
        "no_lf",
        "double_lf",
        "crlf",
        "leading_space",
        "trailing_space",
        "inner_space",
    ),
)
def test_record_reader_never_normalizes_corrupted_canonical_json(
    tmp_path, generated_seal, corruption
):
    canonical = generated_seal.to_json().encode("ascii")
    corrupt = {
        "no_lf": canonical,
        "double_lf": canonical + b"\n\n",
        "crlf": canonical + b"\r\n",
        "leading_space": b" " + canonical + b"\n",
        "trailing_space": canonical + b" \n",
        "inner_space": canonical.replace(b":", b": ", 1) + b"\n",
    }[corruption]
    path = tmp_path / "seal.json"
    path.write_bytes(corrupt)
    with pytest.raises(ValueError):
        type(generated_seal).from_json(read_record(path))
    assert path.read_bytes() == corrupt
    if corruption == "inner_space":
        assert read_record(path).encode("ascii") == corrupt[:-1]


@pytest.fixture
def independent_plugin():
    path = (
        Path(__file__).parents[1]
        / "fixtures/broker_sdk11_plugin/src/histdatacom_generated_sdk11/plugin.py"
    )
    return runpy.run_path(str(path))


def test_independent_sdk11_really_requires_new_method_dispatch(
    independent_plugin,
):
    plugin = independent_plugin["factory"]()
    with pytest.raises(RuntimeError, match="not dispatched"):
        plugin.open_session({"mode": "honest-health"})
    assert plugin.provider_protocol() == independent_plugin["INFO"]
    assert plugin.protocol_calls == 1
    session = plugin.open_session({"mode": "honest-health"})
    plugin.subscribe(session, ("EURUSD",))
    events = tuple(plugin.iter_events(session))
    assert [event.kind for event in events] == [BrokerEventKind.HEALTH] + [
        BrokerEventKind.QUOTE
    ] * 3
    assert all(
        event.source_time is None and event.receive_time is not None
        for event in events
    )
    assert plugin.metadata.sdk_version == "1.1.0"
    plugin.unsubscribe(session, ("EURUSD",))
    plugin.close_session(session)


@pytest.mark.parametrize(
    "factory,kind",
    (
        ("missing_factory", "missing"),
        ("wrong_factory", "wrong"),
        ("mismatch_factory", "mismatch"),
    ),
)
def test_independent_new_method_negatives_are_real_behavior(
    independent_plugin, factory, kind
):
    plugin = independent_plugin[factory]()
    if kind == "missing":
        assert plugin.provider_protocol is None
    elif kind == "wrong":
        assert type(plugin.provider_protocol()) is str
    else:
        assert plugin.provider_protocol() != independent_plugin["INFO"]


def test_prospective_health_policy_does_not_require_or_invent_known_upstream_loss():
    policy = health_policy()
    assert policy.require_known_upstream_loss is False
    assert policy.max_duplicate_rate == policy.max_known_drop_rate == 0
    assert policy.max_clock_jump_ns == 1_000_000_000
    assert policy.max_persistence_p95_ns == 60_000_000_000
    assert policy.require_queue_observations
    assert policy.max_buckets == policy.max_observations == 256


def test_positive_negative_control_does_not_swallow_programming_errors():
    with pytest.raises(AssertionError, match="unexpectedly admitted"):
        require_refusal(lambda: None)
    with pytest.raises(TypeError):
        require_refusal(lambda: len(None))
    assert require_refusal(lambda: int("refuse"))["type"] == "ValueError"


def test_policy_negative_does_not_credit_unrelated_semantic_errors():
    with pytest.raises(ValueError):
        require_policy_refusal(lambda: int("not-a-policy-refusal"))
    with pytest.raises(AssertionError, match="unexpectedly admitted"):
        require_policy_refusal(lambda: None)


def test_tamper_negative_requires_actual_native_integrity_error():
    from histdatacom.broker_plugin_lifecycle import (
        BrokerLifecycleError,
        BrokerLifecycleReason,
    )
    from histdatacom.broker_plugin_policy import BrokerPolicyError

    def refuse(error):
        raise error

    valid = require_integrity_refusal(
        lambda: refuse(BrokerLifecycleError(BrokerLifecycleReason.INTEGRITY))
    )
    assert valid["reason"] == BrokerLifecycleReason.INTEGRITY.value
    with pytest.raises(AssertionError):
        require_integrity_refusal(
            lambda: refuse(
                BrokerLifecycleError(BrokerLifecycleReason.PERSISTENCE)
            )
        )
    with pytest.raises(BrokerPolicyError):
        require_integrity_refusal(lambda: refuse(BrokerPolicyError("denied")))
    with pytest.raises(ValueError):
        require_integrity_refusal(lambda: refuse(ValueError("unrelated")))
    with pytest.raises(AssertionError):
        require_integrity_refusal(lambda: None)


def test_provenance_check_uses_public_enum_and_preserves_all_anchor_checks():
    from histdatacom.broker_plugin_provenance import (
        BrokerProvenanceVerificationReason as Reason,
    )

    seal = object()
    correct = {
        "anchored": True,
        "complete": True,
        "seal": seal,
        "reason": Reason.VERIFIED,
    }
    assert Reason.VERIFIED.value == "verified_anchored_structure"
    check_anchored_provenance(SimpleNamespace(**correct), seal)
    for key, value in (
        ("anchored", False),
        ("complete", False),
        ("seal", object()),
        ("reason", Reason.UNANCHORED),
        ("reason", "verified"),
    ):
        with pytest.raises(AssertionError):
            check_anchored_provenance(
                SimpleNamespace(**{**correct, key: value}), seal
            )


@pytest.mark.parametrize(
    "case", ("other_reason", "other_type", "admitted", "output")
)
def test_conformance_negative_retains_failures_before_asserting(tmp_path, case):
    receipt = tmp_path / "negative.json"
    output = tmp_path / "native"

    def operation():
        if case == "admitted":
            return
        if case == "output":
            output.mkdir()
        if case == "other_type":
            raise TypeError("exact conformance refusal")
        if case == "other_reason":
            raise ValueError("unrelated refusal")
        raise ValueError("exact conformance refusal")

    with pytest.raises(AssertionError):
        recorded_conformance_refusal(
            receipt, output, operation, ValueError, "exact conformance refusal"
        )
    retained = json.loads(receipt.read_text())
    assert retained["refused"] is (case != "admitted")
    assert retained["created_output_paths"] == (
        ["native"] if case == "output" else []
    )
    if case == "other_reason":
        assert retained["reason"] == "unrelated refusal"
    if case == "other_type":
        assert retained["type"] == "TypeError"


def test_numeric_check_rejects_supported_missing_source_facts():
    oracle = {
        "count": 3,
        "interval_count": 2,
        "spread": 0.1,
        "price_decimal_places": 5.0,
        "quote_interarrival_ns": 2.0,
        "support_start": 10,
        "support_end": 14,
    }

    def metric(name, count, estimate):
        return SimpleNamespace(
            name=name,
            support_count=count,
            sample_count=count,
            estimate=estimate,
            limitations=("unsupported",),
        )

    metrics = [
        metric("spread", 3, 0.1),
        metric("price_decimal_places", 3, 5.0),
        metric("quote_interarrival_ns", 2, 2.0),
        metric("source_timestamp_precision_ns", 0, None),
        metric("source_batch_quote_count", 0, None),
    ]
    cell = SimpleNamespace(support_count=3, metrics=metrics)
    fingerprint = SimpleNamespace(
        support_start_utc_ns=10, support_end_utc_ns=14, cells=(cell, cell)
    )
    check_numeric(fingerprint, oracle)
    metrics[-1].support_count = 1
    with pytest.raises(AssertionError):
        check_numeric(fingerprint, oracle)
    metrics[-1].support_count = 0
    metrics[-1].estimate = 1.0
    with pytest.raises(AssertionError):
        check_numeric(fingerprint, oracle)


def test_installed_driver_refuses_checkout_origin():
    with pytest.raises(AssertionError, match="installed host"):
        installed_host("0" * 64)


def test_oracle_reads_quote_bytes_and_host_receive_order(independent_plugin):
    plugin = independent_plugin["factory"]()
    plugin.provider_protocol()
    session = plugin.open_session({"mode": "honest-health"})
    plugin.subscribe(session, ("EURUSD",))
    records = tuple(
        SimpleNamespace(
            kind="event",
            epoch=0,
            receive_monotonic_ns=100 + index * 2,
            receive_utc_ns=1000 + index * 2,
            payload_json=BrokerAdmittedEventV1(
                "broker-capability-plan:sha256:" + "0" * 64,
                event,
                (),
            ).to_json(),
        )
        for index, event in enumerate(plugin.iter_events(session))
    )
    assert quote_oracle(records) == {
        "count": 3,
        "interval_count": 2,
        "spread": 0.10000000000000002,
        "price_decimal_places": 5.0,
        "quote_interarrival_ns": 2.0,
        "support_start": 1002,
        "support_end": 1006,
    }
    original_payload = records[-1].payload_json
    admitted = BrokerAdmittedEventV1.from_json(original_payload)
    records[-1].payload_json = replace(
        admitted,
        event=replace(
            admitted.event, connection_id="other-generated-connection"
        ),
    ).to_json()
    with pytest.raises(AssertionError, match="one generated quote connection"):
        quote_oracle(records)
    records[-1].payload_json = original_payload
    records[-1].receive_monotonic_ns = 1
    with pytest.raises(AssertionError):
        quote_oracle(records)


@pytest.fixture
def generated_admission(monkeypatch):
    from histdatacom.broker_plugin_conformance import (
        BrokerConformanceDriverV1,
        BrokerConformanceReportV1,
    )
    from histdatacom.broker_plugin_permissions import BrokerPermissionManifestV1
    from histdatacom.broker_plugin_registry import (
        BrokerPluginCandidateV1,
        BrokerPluginInventoryV1,
        BrokerPluginRegistrationV1,
    )
    from tests.fixtures import broker_sdk_scientific_qualification as fixture

    root = Path(__file__).parents[1] / "fixtures/broker_sdk11_plugin"
    package = root / "src/histdatacom_generated_sdk11"
    registration_text = (
        package / "_histdatacom_broker_plugins/org.example.sdk11.json"
    ).read_text()
    candidate = BrokerPluginCandidateV1(
        BrokerPluginRegistrationV1.from_json(registration_text),
        hashlib.sha256(registration_text.encode()).hexdigest(),
        hashlib.sha256((package / "plugin.py").read_bytes()).hexdigest(),
    )
    permission = BrokerPermissionManifestV1.from_json(
        (
            package / "_histdatacom_broker_permissions/org.example.sdk11.json"
        ).read_text()
    )
    driver = BrokerConformanceDriverV1.from_json(
        (root / "conformance/org.example.sdk11.json").read_text()
    )
    assert driver.candidate_id == candidate.artifact_id
    assert permission.candidate_id == candidate.artifact_id
    assert permission.sdk_version == "1.1.0"

    def installed_permission(selected, *, sdk_version):
        assert selected == candidate and sdk_version == "1.1.0"
        return permission

    monkeypatch.setattr(
        fixture, "read_installed_broker_permissions", installed_permission
    )
    monkeypatch.setattr(
        "histdatacom.broker_plugin_conformance.drivers.read_installed_broker_permissions",
        installed_permission,
    )
    inventory = BrokerPluginInventoryV1((candidate,), sdk_version="1.1.0")
    request, retained = fixture.invocation(inventory, driver, "1.1.0")
    assert (
        retained == permission and request.output_contract.allow_opaque_metadata
    )
    reference = fixture.conformance_reference(inventory, driver)
    report = BrokerConformanceReportV1.from_json(reference.report_json)
    assert not report.certified and reference.evidence_ids == ()
    assert all(item.status.value == "not_run" for item in report.results)
    wrong = BrokerConformanceReportV1.from_json(
        fixture.wrong_conformance(reference).report_json
    )
    assert wrong.plan.driver.candidate_id != candidate.artifact_id
    return inventory, request, reference


def test_generated_metadata_request_and_honest_conformance_reference(
    monkeypatch, tmp_path, generated_admission
):
    from tests.fixtures import broker_sdk_scientific_qualification as fixture

    inventory, request, reference = generated_admission
    candidate = request.plan.candidate

    # Exercise the REAL public lifecycle validation and secure error sanitizer.
    # Only trusted security preflight is synthetic; never start a worker here.
    from histdatacom.broker_plugin_capabilities import execution as capabilities
    from histdatacom.broker_plugin_security import execution

    validated_requests = []
    validate_request = capabilities._provider_request
    secure_lifecycle_errors = []
    actual_lifecycle = execution.run_broker_plugin_lifecycle

    def observed_lifecycle(*args, **kwargs):
        try:
            return actual_lifecycle(*args, **kwargs)
        except ValueError as error:
            secure_lifecycle_errors.append((type(error), str(error)))
            raise

    def recorded_request(plan, supplied):
        validated = validate_request(plan, supplied)
        validated_requests.append(validated)
        return validated

    def no_native_work(*args, **kwargs):
        raise AssertionError("wrong candidate passed conformance validation")

    monkeypatch.setattr(capabilities, "_provider_request", recorded_request)
    monkeypatch.setattr(
        execution, "run_broker_plugin_lifecycle", observed_lifecycle
    )
    monkeypatch.setattr(
        "histdatacom.broker_plugin_capabilities.execution._provider_call",
        no_native_work,
    )
    monkeypatch.setattr(execution, "_preflight", lambda *args: request)
    monkeypatch.setattr(
        execution,
        "prepare_kernel_launch",
        lambda *args: SimpleNamespace(
            command_prefix=(),
            environment={},
            working_directory=tmp_path,
            bootstrap_source="",
        ),
    )
    monkeypatch.setattr(
        execution,
        "_private_inputs",
        lambda *args: (
            {},
            SimpleNamespace(check=lambda value: None),
            None,
            "{}",
        ),
    )
    negative = fixture.conformance_negatives(
        tmp_path,
        inventory,
        request,
        reference,
        fixture.BrokerSecurityPolicyV1(
            candidate.artifact_id,
            fixture.BrokerTrustTier.DEVELOPMENT,
            fixture.BrokerSecurityMode.KERNEL_ISOLATED,
            fixture.BrokerNetworkMode.OFF,
        ),
    )
    assert validated_requests == [request, request]
    assert secure_lifecycle_errors == [
        (
            ValueError,
            "capture conformance evidence belongs to another candidate",
        )
    ]
    assert negative["lifecycle"]["type"] == "ValueError"
    assert negative["lifecycle"]["reason"] == (
        "capture conformance evidence belongs to another candidate"
    )
    assert negative["secure"]["type"] == "BrokerSecurityError"
    assert negative["secure"]["reason"] == "security_execution_failed"
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "conformance-lifecycle-negative.json",
        "conformance-secure-negative.json",
    ]


def test_verify_uses_separate_real_positive_and_denied_policy_scopes(
    monkeypatch, tmp_path, generated_admission
):
    """Mock expensive evidence only; exercise the real scope and rights guards."""
    import histdatacom.broker_capture as capture_api
    import histdatacom.broker_plugin_compatibility as compatibility_api
    from histdatacom.broker_plugin_policy import (
        BrokerPolicyError,
        BrokerPolicyOperation,
        require_provider_operation,
    )
    from histdatacom.broker_plugin_policy.scope import (
        read_current_provider_policy_context,
    )
    from tests.fixtures import broker_sdk_scientific_qualification as fixture

    _, request, reference = generated_admission
    effects = []

    class SyntheticFingerprint:
        fingerprint_id = "unit-control-flow-not-installed-evidence"

        def to_json(self):
            return "synthetic-fingerprint"

    fingerprint = SyntheticFingerprint()
    manifest = SimpleNamespace(
        to_json=lambda: "synthetic-manifest",
        worker_reaped=True,
        completion=SimpleNamespace(value="complete_local_finite_run"),
        forced_terminations=0,
        error_diagnostics=0,
        discarded_known=0,
        header=SimpleNamespace(inventory=SimpleNamespace(sdk_version="1.0.0")),
    )
    seal, security = object(), object()
    health = SimpleNamespace(
        header=SimpleNamespace(policy=fixture.health_policy()),
        to_json=lambda: "synthetic-health",
        state=fixture.BrokerHostHealthState.QUALIFIED,
        failures=(),
        complete_observations=True,
        artifact_id="synthetic-health-id",
    )
    seal = SimpleNamespace(artifact_id="synthetic-seal-id")
    evidence = SimpleNamespace(
        anchored=True,
        complete=True,
        seal=seal,
        reason=fixture.BrokerProvenanceVerificationReason.VERIFIED,
    )
    compatibility = SimpleNamespace(
        declaration=SimpleNamespace(
            conformance=reference,
            provider_protocol_status="not_yet_observed_see_sealed_runtime_metadata",
        ),
        provider_protocol_jsons=(),
        artifact_id="synthetic-compatibility-id",
    )
    retained = {
        "manifest.json": "synthetic-manifest",
        "seal.json": "synthetic-seal",
        "security.json": "synthetic-security",
        "returned-health.json": "synthetic-health",
        "conformance-reference.json": reference.to_json(),
    }
    monkeypatch.setattr(
        fixture, "read_record", lambda path: retained[path.name]
    )
    for name, value in (
        ("BrokerLifecycleManifestV1", manifest),
        ("BrokerProvenanceSealV1", seal),
        ("BrokerSecurityReceiptV1", security),
    ):
        monkeypatch.setattr(
            fixture, name, SimpleNamespace(from_json=lambda text, v=value: v)
        )
    monkeypatch.setattr(
        fixture,
        "inspect_broker_lifecycle",
        lambda native: SimpleNamespace(manifest=manifest, complete=True),
    )
    monkeypatch.setattr(fixture, "replay_broker_lifecycle", lambda *a, **kw: ())
    monkeypatch.setattr(
        fixture, "read_lifecycle_host_health", lambda *a: health
    )
    monkeypatch.setattr(
        fixture, "read_lifecycle_capture_provenance", lambda *a, **kw: evidence
    )
    monkeypatch.setattr(fixture, "verify_security_capture", lambda *a: None)
    monkeypatch.setattr(
        compatibility_api,
        "read_lifecycle_capture_compatibility",
        lambda *a, **kw: compatibility,
    )
    monkeypatch.setattr(fixture, "quote_oracle", lambda records: {})
    monkeypatch.setattr(fixture, "check_numeric", lambda *a: None)
    monkeypatch.setattr(
        capture_api, "BrokerDeliveryFingerprintV3", SyntheticFingerprint
    )

    def current_selection():
        return read_current_provider_policy_context().selected_policy_ids

    @contextmanager
    def material_scope(*args):
        selection = current_selection()
        effects.append(("material-enter", selection))
        yield
        assert current_selection() == selection
        effects.append(("material-exit", selection))

    monkeypatch.setattr(
        capture_api, "broker_fingerprint_sources", material_scope
    )
    monkeypatch.setattr(fixture, "provider_native_inputs", material_scope)

    def fit(*args, **kwargs):
        effects.append(("fit", current_selection()))
        require_provider_operation(request, BrokerPolicyOperation.DERIVE)
        return fingerprint

    def transfer(*args):
        effects.append(("transfer", current_selection()))
        require_provider_operation(request, BrokerPolicyOperation.MATERIAL_USE)
        return {"mock_control_flow_only": True}

    monkeypatch.setattr(capture_api, "fit_broker_delivery_fingerprint", fit)
    monkeypatch.setattr(fixture, "transfer", transfer)
    monkeypatch.setattr(
        capture_api, "write_broker_delivery_fingerprint", lambda *a: None
    )
    monkeypatch.setattr(
        capture_api, "load_broker_delivery_fingerprint", lambda *a: fingerprint
    )
    result = fixture.verify(tmp_path, request, offline=False)
    fit_scopes = [scope for operation, scope in effects if operation == "fit"]
    transfer_scopes = [
        scope for operation, scope in effects if operation == "transfer"
    ]
    assert len(fit_scopes) == 3 and fit_scopes[0] == fit_scopes[1]
    assert fit_scopes[2] != fit_scopes[0]
    assert transfer_scopes == [fit_scopes[0], fit_scopes[2]]
    assert [kind for kind, scope in effects if scope == fit_scopes[0]][-2:] == [
        "material-exit",
        "material-exit",
    ]
    for key in ("denied_fit", "denied_transfer"):
        assert result[key] == {
            "type": "BrokerPolicyError",
            "reason": "provider policy refused: operation_not_allowed",
        }
    with pytest.raises(
        BrokerPolicyError, match="current_process_policy_scope_required"
    ):
        read_current_provider_policy_context()

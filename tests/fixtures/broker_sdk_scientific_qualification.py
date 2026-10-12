"""Installed SDK-native generated science acceptance, never empirical evidence.

Copy this file and the exact conformance driver outside the checkout. The caller
installs real host/plugin wheels, runs ``capture``, uninstalls the plugin, then
runs ``offline`` in a NEW process. This script never installs packages or adds
checkout paths. Both phases preserve evidence; failed captures are not retried
with different health thresholds. The SDK 1.1 fixture is a separate distribution.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import secrets
import shutil
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from itertools import pairwise
from pathlib import Path

import histdatacom
from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_conformance import BrokerConformanceDriverV1
from histdatacom.broker_plugin_health import (
    BrokerHostHealthPolicyV1,
    BrokerHostHealthState,
)
from histdatacom.broker_plugin_health.storage import read_lifecycle_host_health
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleError,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleReason,
    inspect_broker_lifecycle,
    replay_broker_lifecycle,
    run_broker_plugin_lifecycle,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionGrantV1,
    BrokerPermissionResourcesV1,
    broker_permission_scope,
    read_installed_broker_permissions,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyAcknowledgementV1,
    BrokerPolicyConstraintMode,
    BrokerPolicyConstraintV1,
    BrokerPolicyContextV1,
    BrokerPolicyDataClass,
    BrokerPolicyEvidenceKind,
    BrokerPolicyEvidenceV1,
    BrokerPolicyExecutionV1,
    BrokerPolicyOperation,
    BrokerPolicyReferenceV1,
    BrokerPolicyRetention,
    BrokerPolicyRuleV1,
    BrokerPolicyStatus,
    BrokerProviderConfigurationV1,
    BrokerProviderOutputContractV1,
    BrokerProviderPolicyV1,
    BrokerSDKInvocationV1,
    BrokerSDKSecurityV1,
    provider_native_inputs,
    provider_policy_scope,
    require_provider_operation,
    sdk_invocation_binding,
)
from histdatacom.broker_plugin_provenance import (
    BrokerProvenanceSealV1,
    BrokerProvenanceVerificationReason,
    read_lifecycle_capture_provenance,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_security import (
    BrokerNetworkMode,
    BrokerSecurityError,
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerSecurityReason,
    BrokerSecurityReceiptV1,
    BrokerTrustTier,
    run_secure_broker_plugin,
    verify_security_capture,
)
from histdatacom.broker_plugins import BrokerEventKind

SECOND = 1_000_000_000
NONCLAIM = (
    "Generated transport and synthetic authority only; no provider entitlement, "
    "authenticated protocol, real account, empirical equivalence, throughput "
    "benchmark, or known upstream loss claim."
)
OPERATIONS = tuple(
    sorted(
        (
            "configuration_schema",
            "open_session",
            "instruments",
            "subscribe",
            "iter_events",
            "unsubscribe",
            "close_session",
        )
    )
)


def health_policy():
    """Prospective generous storage latency bound, NOT a performance result."""
    return BrokerHostHealthPolicyV1(
        max_observations=256,
        max_buckets=256,
        max_samples_per_bucket=64,
        max_persistence_p95_ns=60 * SECOND,
        max_clock_jump_ns=SECOND,
        require_known_upstream_loss=False,
    )


def create_once(path, value):
    text = (
        value if isinstance(value, str) else json.dumps(value, sort_keys=True)
    )
    with path.open("x", encoding="ascii") as stream:
        stream.write(text + "\n")


def read_record(path):
    """Remove exactly this fixture's single LF frame, not JSON whitespace."""
    data = path.read_bytes()
    if not data.endswith(b"\n"):
        raise ValueError("fixture record requires one terminal LF")
    text = data[:-1].decode("ascii")
    if not text or text[0].isspace() or text[-1].isspace():
        raise ValueError("fixture record has unexpected surrounding whitespace")
    return text


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def installed_wheel(name, expected_sha):
    distribution = importlib.metadata.distribution(name)
    direct = json.loads(distribution.read_text("direct_url.json") or "{}")
    if direct.get("dir_info", {}).get("editable"):
        raise AssertionError("editable installations are not qualification")
    hashes = direct.get("archive_info", {}).get("hashes", {})
    assert hashes.get("sha256") == expected_sha, (name, "wheel identity")
    return {"name": name, "version": distribution.version, "direct_url": direct}


def installed_host(expected_sha):
    origin = Path(histdatacom.__file__).resolve()
    assert "site-packages" in origin.parts, "installed host required"
    assert Path(sys.prefix).resolve() in origin.parents
    assert not any(
        (Path(item or ".") / "src/histdatacom").is_dir() for item in sys.path
    ), "checkout import path present"
    return installed_wheel("histdatacom", expected_sha)


@dataclass
class PolicySource:
    context: BrokerPolicyContextV1

    def read_policy_context(self):
        return BrokerPolicyContextV1.from_json(self.context.to_json())


@dataclass
class PermissionSource:
    context: BrokerPermissionContextV1

    def read_context(self):
        return BrokerPermissionContextV1.from_json(self.context.to_json())


def current_policy(request, *, denied=False, offline=False):
    now = time.time_ns()
    terms = b"Explicit generated science fixture authority; no provider rights."
    reference = BrokerPolicyReferenceV1(
        "generated-terms",
        "sdk-science-v1",
        hashlib.sha256(terms).hexdigest(),
        len(terms),
    )
    evidence = BrokerPolicyEvidenceV1(
        reference,
        request.configuration_profile.provider_id,
        "sdk-science",
        "1",
        datetime.fromtimestamp(now // SECOND, timezone.utc).date().isoformat(),
        "generated-fixture",
        now,
        BrokerPolicyEvidenceKind.DECLARED,
        "fixture:generated-sdk-science",
    )
    operations = {
        BrokerPolicyOperation.MATERIAL_USE,
        BrokerPolicyOperation.DERIVE,
        BrokerPolicyOperation.RETAIN_LOCAL,
    }
    if not offline:
        operations.update(
            (BrokerPolicyOperation.INVOKE, BrokerPolicyOperation.CAPTURE)
        )
    rules = []
    for operation in sorted(BrokerPolicyOperation):
        for data_class in sorted(BrokerPolicyDataClass):
            allowed = not denied and operation in operations
            rules.append(
                BrokerPolicyRuleV1(
                    operation,
                    data_class,
                    (
                        BrokerPolicyStatus.ALLOWED
                        if allowed
                        else BrokerPolicyStatus.DENIED
                    ),
                    (evidence.artifact_id,),
                    (
                        (
                            BrokerPolicyRetention.UNBOUNDED
                            if allowed
                            else BrokerPolicyRetention.UNKNOWN
                        )
                        if operation is BrokerPolicyOperation.RETAIN_LOCAL
                        else BrokerPolicyRetention.NOT_APPLICABLE
                    ),
                )
            )
    policy = BrokerProviderPolicyV1(
        "1.0.0",
        sdk_invocation_binding(request),
        "MIT",
        "MIT",
        (evidence.artifact_id,),
        tuple(rules),
        tuple(
            BrokerPolicyConstraintV1(name, BrokerPolicyConstraintMode.ANY)
            for name in (
                "account_class",
                "commercial_use",
                "eligibility",
                "feed_type",
                "geography",
            )
        ),
        (),
        now,
        now,
        now + 3600 * SECOND,
    )
    acknowledgement = BrokerPolicyAcknowledgementV1(
        policy.artifact_id,
        (evidence.artifact_id,),
        reference,
        now,
        now + 3600 * SECOND,
    )
    return PolicySource(
        BrokerPolicyContextV1(
            (policy,),
            (evidence,),
            (acknowledgement,),
            (),
            (policy.artifact_id,),
            BrokerPolicyExecutionV1(False, "generated", "fixture", "fixture"),
        )
    )


def invocation(inventory, driver, sdk):
    candidates = [
        item
        for item in inventory.candidates
        if item.artifact_id == driver.candidate_id
    ]
    assert len(candidates) == 1, "exact installed candidate required"
    candidate = candidates[0]
    permission = read_installed_broker_permissions(candidate, sdk_version=sdk)
    assert permission.artifact_id == driver.permission_manifest_id
    assert permission.sdk_version == sdk
    assert not (
        permission.endpoints or permission.caches or permission.secret_profiles
    )
    assert permission.subprocess_mode == "none"
    required = (
        "events.v1",
        "health.v1",
        "instruments.v1",
        "quotes.v1",
        "session.v1",
    )
    optional = tuple(
        sorted(set(candidate.registration.capabilities) - set(required))
    )
    workflow = BrokerCapabilityWorkflowV1(OPERATIONS, required, optional)
    plan = negotiate_broker_capabilities(
        inventory,
        workflow,
        plugin_id=candidate.registration.plugin_id,
        version_constraint="==" + candidate.registration.plugin_version,
    )
    plan.require_admitted()
    request = BrokerSDKInvocationV1(
        plan,
        BrokerProviderConfigurationV1(
            driver.provider_id,
            "generated-sdk-science",
            driver.configuration_schema_json,
            '{"mode":"honest-health"}',
        ),
        BrokerProviderOutputContractV1(
            "sdk-v1",
            ("health", "quote"),
            allow_opaque_metadata=sdk == "1.1.0",
        ),
    )
    return request, permission


def authority(request, permission):
    now = time.time_ns()
    atoms = ("emit:health", "emit:quotes")
    if permission.sdk_version == "1.1.0":
        atoms += ("raw_payload:emit",)
    assert set(permission.required_atoms) <= set(atoms)
    assert set(atoms) <= set(
        permission.required_atoms + permission.optional_atoms
    )
    binding = BrokerPermissionBindingV1(
        request.plan.candidate.artifact_id,
        permission.artifact_id,
        permission.sdk_version,
        request.configuration_profile.provider_id,
        request.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding,
        atoms,
        "generated-fixture",
        now,
        now + 3600 * SECOND,
        secrets.token_hex(16),
    )
    return BrokerPermissionAuthorityV1(
        permission,
        binding,
        grant.artifact_id,
        PermissionSource(BrokerPermissionContextV1((grant,))),
    )


def conformance_reference(inventory, driver):
    """Retain an honest unexecuted exact-candidate report, not certification."""
    from histdatacom.broker_plugin_compatibility import (
        BrokerConformanceReferenceV1,
    )
    from histdatacom.broker_plugin_conformance import (
        BrokerConformanceCaseResultV1,
        BrokerConformanceProfile,
        BrokerConformanceReason,
        BrokerConformanceStatus,
        broker_conformance_catalog,
        plan_broker_conformance,
    )
    from histdatacom.broker_plugin_conformance.assessment import (
        assess_broker_conformance,
    )

    catalog = broker_conformance_catalog()
    plan = plan_broker_conformance(
        inventory,
        driver,
        profile=BrokerConformanceProfile.TRUSTED,
        capabilities=("quotes.v1",),
    )
    results = tuple(
        BrokerConformanceCaseResultV1(
            case,
            BrokerConformanceStatus.NOT_RUN,
            BrokerConformanceReason.DRIVER,
            plan.subject,
        )
        for case in plan.case_ids
    )
    report = assess_broker_conformance(plan, results)
    assert not report.certified
    return BrokerConformanceReferenceV1(
        catalog.version,
        catalog.to_json(),
        report.to_json(),
        (),
    )


def wrong_conformance(reference):
    from histdatacom.broker_plugin_conformance import BrokerConformanceReportV1

    report = BrokerConformanceReportV1.from_json(reference.report_json)
    altered = replace(
        report,
        plan=replace(
            report.plan,
            driver=replace(
                report.plan.driver,
                candidate_id="broker-plugin-candidate:sha256:" + "0" * 64,
            ),
        ),
    )
    return replace(reference, report_json=altered.to_json())


def recorded_conformance_refusal(
    receipt, output, operation, expected_type, expected_reason
):
    """Retain the actual negative outcome before checking its exact cause."""
    try:
        operation()
    except Exception as error:  # noqa: BLE001
        # Retain every ordinary failure, then reject any unexpected type/cause.
        report = {
            "type": type(error).__name__,
            "reason": str(error),
            "refused": True,
            "created_output_paths": sorted(
                path.name for path in output.parent.glob(output.name + "*")
            ),
        }
        create_once(receipt, report)
        assert type(error) is expected_type, report
        assert str(error) == expected_reason, report
        assert not report["created_output_paths"], report
        return report
    report = {
        "type": None,
        "reason": "negative control unexpectedly admitted",
        "refused": False,
        "created_output_paths": sorted(
            path.name for path in output.parent.glob(output.name + "*")
        ),
    }
    create_once(receipt, report)
    raise AssertionError(report)


def conformance_negatives(root, inventory, request, conformance, security):
    """Exact lifecycle cause and separately sanitized secure-wrapper refusal."""
    wrong = wrong_conformance(conformance)
    lifecycle_output = root / "wrong-conformance-lifecycle-native"
    lifecycle = recorded_conformance_refusal(
        root / "conformance-lifecycle-negative.json",
        lifecycle_output,
        lambda: run_broker_plugin_lifecycle(
            inventory,
            request.plan,
            {"mode": "honest-health"},
            ("EURUSD",),
            lifecycle_output,
            authorize=lambda value: value == request.plan,
            provider_request=request,
            conformance_reference=wrong,
        ),
        ValueError,
        "capture conformance evidence belongs to another candidate",
    )
    secure_output = root / "wrong-conformance-secure-native"
    secure = recorded_conformance_refusal(
        root / "conformance-secure-negative.json",
        secure_output,
        lambda: run_secure_broker_plugin(
            inventory,
            request.plan,
            security,
            {"mode": "honest-health"},
            ("EURUSD",),
            secure_output,
            authorize=lambda value: value == security,
            provider_request=request,
            protected_paths=(root,),
            conformance_reference=wrong,
        ),
        BrokerSecurityError,
        BrokerSecurityReason.EXECUTION.value,
    )
    return {"lifecycle": lifecycle, "secure": secure}


def quote_oracle(records):
    """Independent direct arithmetic: do not call fitter metric helpers."""
    quotes = []
    connections = set()
    for record in records:
        if record.kind != "event":
            continue
        event = BrokerAdmittedEventV1.from_json(record.payload_json).event
        if event.kind is BrokerEventKind.QUOTE:
            assert event.quote is not None and event.receive_time is not None
            assert event.source_time is None, "generated source time is unknown"
            assert event.raw_provenance is None and not event.extensions
            assert event.quote.bid_size is None and event.quote.ask_size is None
            assert event.quote.activity is None
            connections.add(event.connection_id)
            quotes.append((record, event.quote))
    assert len(quotes) == 3, "exact fresh generated quote stream required"
    assert all(quote.symbol == "EURUSD" for _, quote in quotes)
    mono = [record.receive_monotonic_ns for record, _ in quotes]
    assert all(right >= left for left, right in pairwise(mono))
    assert len({record.epoch for record, _ in quotes}) == 1
    assert len(connections) == 1, "one generated quote connection required"
    spread = [
        float(Decimal(quote.ask) - Decimal(quote.bid)) for _, quote in quotes
    ]
    precision = [
        max(len(price.partition(".")[2]) for price in (q.bid, q.ask))
        for _, q in quotes
    ]
    intervals = [right - left for left, right in pairwise(mono) if right > left]
    assert intervals, "positive host receive interval support required"
    return {
        "count": len(quotes),
        "spread": sum(spread) / len(spread),
        "price_decimal_places": sum(precision) / len(precision),
        "quote_interarrival_ns": sum(intervals) / len(intervals),
        "interval_count": len(intervals),
        "support_start": min(record.receive_utc_ns for record, _ in quotes),
        "support_end": max(record.receive_utc_ns for record, _ in quotes),
    }


def check_numeric(fingerprint, oracle):
    assert fingerprint.support_start_utc_ns == oracle["support_start"]
    assert fingerprint.support_end_utc_ns == oracle["support_end"]
    assert len(fingerprint.cells) == 2
    for cell in fingerprint.cells:
        assert cell.support_count == oracle["count"]
        metrics = {item.name: item for item in cell.metrics}
        for name in ("spread", "price_decimal_places", "quote_interarrival_ns"):
            item = metrics[name]
            count = (
                oracle["interval_count"]
                if name == "quote_interarrival_ns"
                else oracle["count"]
            )
            assert item.support_count == count and item.sample_count == count
            assert math.isclose(item.estimate, oracle[name], rel_tol=1e-12)
        for name in (
            "source_timestamp_precision_ns",
            "source_batch_quote_count",
        ):
            item = metrics[name]
            assert item.estimate is None
            assert item.support_count == item.sample_count == 0
            assert (
                item.limitations
            ), "missing facts must remain explicitly unsupported"


def transfer(fingerprint, oracle):
    from histdatacom.synthetic import (
        BrokerTransferConfigV1,
        InformationMode,
        ReferenceMotifConditionV1,
        ReferenceMotifQueryV1,
        condition_broker_proposal,
    )

    query = ReferenceMotifQueryV1(
        ReferenceMotifConditionV1(
            "EURUSD",
            "generated-only",
            "generated",
            metrics={"spread": 0.002, "price_precision_digits": 8.0},
        ),
        InformationMode.EX_POST_RECONSTRUCTION,
        fingerprint.support_end_utc_ns,
    )
    result = condition_broker_proposal(
        query,
        fingerprint,
        requested_condition={"symbol": "EURUSD"},
        selected_at_utc_ns=fingerprint.support_end_utc_ns,
        config=BrokerTransferConfigV1(strength=0.5),
    )
    assert result.selection.applied and result.conditioned_query is not None
    assert result.conditioned_query.query_id != query.query_id
    assert math.isclose(
        result.metrics_after["spread"],
        (0.002 + oracle["spread"]) / 2,
        abs_tol=1e-12,
    )
    assert math.isclose(
        result.metrics_after["price_precision_digits"],
        (8 + oracle["price_decimal_places"]) / 2,
        abs_tol=1e-12,
    )
    assert "timestamp_precision_ns" not in result.metrics_after
    return result.to_dict()


def require_refusal(operation):
    try:
        operation()
    except (ValueError, OSError) as error:
        return {"type": type(error).__name__, "reason": str(error)}
    raise AssertionError("negative control unexpectedly admitted")


def require_policy_refusal(operation):
    from histdatacom.broker_plugin_policy import BrokerPolicyError

    try:
        operation()
    except BrokerPolicyError as error:
        return {"type": type(error).__name__, "reason": str(error)}
    raise AssertionError("current rights denial unexpectedly admitted")


def require_integrity_refusal(operation):
    try:
        operation()
    except BrokerLifecycleError as error:
        assert error.reason is BrokerLifecycleReason.INTEGRITY
        return {"type": type(error).__name__, "reason": str(error)}
    raise AssertionError("tampered native replay unexpectedly admitted")


def check_anchored_provenance(verified, seal):
    assert verified.anchored and verified.complete and verified.seal == seal
    assert verified.reason is BrokerProvenanceVerificationReason.VERIFIED


def verify(root, request, *, offline):
    from histdatacom.broker_capture import (
        BrokerDeliveryFingerprintV3,
        BrokerLifecycleFingerprintFitConfigV1,
        broker_fingerprint_sources,
        fit_broker_delivery_fingerprint,
        load_broker_delivery_fingerprint,
        write_broker_delivery_fingerprint,
    )
    from histdatacom.broker_plugin_compatibility import (
        BrokerConformanceReferenceV1,
        read_lifecycle_capture_compatibility,
    )
    from histdatacom.broker_plugins import BrokerProviderProtocolV1

    native = root / "native"
    manifest = BrokerLifecycleManifestV1.from_json(
        read_record(root / "manifest.json")
    )
    seal = BrokerProvenanceSealV1.from_json(read_record(root / "seal.json"))
    security = BrokerSecurityReceiptV1.from_json(
        read_record(root / "security.json")
    )
    with provider_policy_scope(current_policy(request, offline=offline)):
        inspection = inspect_broker_lifecycle(native)
        actual = inspection.manifest
        assert actual.to_json() == manifest.to_json()
        assert inspection.complete and actual.worker_reaped
        assert actual.completion.value == "complete_local_finite_run"
        assert not (
            actual.forced_terminations
            or actual.error_diagnostics
            or actual.discarded_known
        )
        records = tuple(
            replay_broker_lifecycle(native, provider_request=request)
        )
        health = read_lifecycle_host_health(
            native.with_name("native-host-health"),
            manifest,
            records,
            request,
        )
        assert health.header.policy == health_policy()
        assert health.to_json() == read_record(root / "returned-health.json")
        assert health.state is BrokerHostHealthState.QUALIFIED
        assert not health.failures and health.complete_observations
        verified = read_lifecycle_capture_provenance(
            native,
            manifest,
            provider_request=request,
            expected_root=seal,
        )
        check_anchored_provenance(verified, seal)
        verify_security_capture(security, manifest)
        compatibility = read_lifecycle_capture_compatibility(
            native,
            manifest,
            provider_request=request,
            expected_root=seal,
        )
        assert compatibility is not None
        assert (
            compatibility.declaration.conformance
            == BrokerConformanceReferenceV1.from_json(
                read_record(root / "conformance-reference.json"),
            )
        )
        assert compatibility.declaration.provider_protocol_status == (
            "not_yet_observed_see_sealed_runtime_metadata"
        )
        if manifest.header.inventory.sdk_version == "1.1.0":
            assert compatibility.provider_protocol_jsons == (
                BrokerProviderProtocolV1(
                    "generated-sdk11",
                    "generated-finite-quotes",
                    "1.0.0",
                ).to_json(),
            )
        else:
            assert compatibility.provider_protocol_jsons == ()
        oracle = quote_oracle(records)
        config = BrokerLifecycleFingerprintFitConfigV1()
        with broker_fingerprint_sources(native):
            fingerprint = fit_broker_delivery_fingerprint(
                native,
                (manifest,),
                provider_requests=(request,),
                config=config,
            )
            assert type(fingerprint) is BrokerDeliveryFingerprintV3
            assert (
                fit_broker_delivery_fingerprint(
                    native,
                    (manifest,),
                    provider_requests=(request,),
                    config=config,
                ).to_json()
                == fingerprint.to_json()
            )
            check_numeric(fingerprint, oracle)
            with provider_native_inputs(fingerprint):
                if not offline:
                    write_broker_delivery_fingerprint(
                        root / "fingerprint.json", fingerprint
                    )
                restored = load_broker_delivery_fingerprint(
                    root / "fingerprint.json"
                )
                assert restored.to_json() == fingerprint.to_json()
                consumer = transfer(restored, oracle)
    # The public policy scope deliberately refuses nesting. Establish a fresh
    # denied scope only after all positive material-use scopes have exited.
    with (
        provider_policy_scope(
            current_policy(request, denied=True, offline=offline)
        ),
        broker_fingerprint_sources(native),
        provider_native_inputs(fingerprint),
    ):
        denied_fit = require_policy_refusal(
            lambda: fit_broker_delivery_fingerprint(
                native,
                (manifest,),
                provider_requests=(request,),
                config=config,
            )
        )
        denied_transfer = require_policy_refusal(
            lambda: transfer(restored, oracle)
        )
    return {
        "fingerprint_id": fingerprint.fingerprint_id,
        "oracle": oracle,
        "health_id": health.artifact_id,
        "seal_id": seal.artifact_id,
        "compatibility_id": compatibility.artifact_id,
        "consumer": consumer,
        "denied_fit": denied_fit,
        "denied_transfer": denied_transfer,
        "nonclaim": NONCLAIM,
    }


def capture(args):
    root = args.output.absolute()
    root.mkdir(mode=0o700)
    installed = installed_host(args.host_wheel_sha256)
    driver = BrokerConformanceDriverV1.from_json(args.driver.read_text())
    inventory = discover_broker_plugins(sdk_version=args.sdk_version)
    request, permission = invocation(inventory, driver, args.sdk_version)
    registration = request.plan.candidate.registration
    plugin = installed_wheel(
        registration.distribution_name, args.plugin_wheel_sha256
    )
    create_once(
        root / "preflight.json",
        {
            "host": installed,
            "plugin": plugin,
            "sdk": args.sdk_version,
            "module": registration.entry_point.split(":")[0].split(".")[0],
            "nonclaim": NONCLAIM,
        },
    )
    create_once(root / "request.json", request.to_json())
    create_once(root / "health-policy.json", health_policy().to_json())
    conformance = conformance_reference(inventory, driver)
    create_once(root / "conformance-reference.json", conformance.to_json())
    permission_authority = authority(request, permission)
    security_policy = BrokerSecurityPolicyV1(
        request.plan.candidate.artifact_id,
        BrokerTrustTier.DEVELOPMENT,
        BrokerSecurityMode.KERNEL_ISOLATED,
        BrokerNetworkMode.OFF,
    )
    with provider_policy_scope(current_policy(request)):
        resources = (
            BrokerPermissionResourcesV1(
                permission_authority, provider_request=request
            )
            if permission.resource_abi == "host_resources_v1"
            else None
        )
        with broker_permission_scope(permission_authority, resources=resources):
            wrong_report = conformance_negatives(
                root, inventory, request, conformance, security_policy
            )
            result = run_secure_broker_plugin(
                inventory,
                request.plan,
                security_policy,
                {"mode": "honest-health"},
                ("EURUSD",),
                root / "native",
                authorize=lambda value: value == security_policy,
                provider_request=request,
                protected_paths=(root,),
                lifecycle_policy=BrokerLifecyclePolicyV1(
                    max_events=16,
                    startup_timeout_ms=30_000,
                    run_timeout_ms=120_000,
                    acknowledgement_timeout_ms=10_000,
                    shutdown_timeout_ms=1_000,
                    delivery_retries=0,
                    retry_delays_ms=(),
                ),
                health_policy=health_policy(),
                conformance_reference=conformance,
            )
        require_provider_operation(
            BrokerSDKSecurityV1(
                request, result.security, result.native.manifest
            ),
            BrokerPolicyOperation.RETAIN_LOCAL,
        )
        create_once(root / "manifest.json", result.native.manifest.to_json())
        create_once(root / "seal.json", result.native.provenance.to_json())
        create_once(root / "security.json", result.security.to_json())
        create_once(
            root / "returned-health.json", result.native.health.to_json()
        )
    report = verify(root, request, offline=False)
    report["wrong_candidate_conformance"] = wrong_report
    # Preserve the original tree; corrupt only a fresh copy and require replay refusal.
    copied = root / "tampered-native"
    shutil.copytree(root / "native", copied)
    partitions = sorted(copied.glob("*.jsonl"))
    assert partitions, "native partition expected"
    with partitions[0].open("ab") as stream:
        stream.write(b"{}\n")
    with provider_policy_scope(current_policy(request)):
        report["tampered_replay"] = require_integrity_refusal(
            lambda: tuple(
                replay_broker_lifecycle(copied, provider_request=request)
            ),
        )
    create_once(root / "capture-report.json", report)
    return report


def offline(args):
    root = args.output.absolute()
    installed_host(args.host_wheel_sha256)
    preflight = json.loads(read_record(root / "preflight.json"))
    try:
        importlib.metadata.distribution(preflight["plugin"]["name"])
    except importlib.metadata.PackageNotFoundError:
        pass
    else:
        raise AssertionError("plugin must be uninstalled in a prior process")
    assert importlib.util.find_spec(preflight["module"]) is None
    assert not any(
        name == preflight["module"]
        or name.startswith(preflight["module"] + ".")
        for name in sys.modules
    )
    before = {
        path.relative_to(root).as_posix(): sha(path)
        for path in root.rglob("*")
        if path.is_file()
    }
    request = BrokerSDKInvocationV1.from_json(
        read_record(root / "request.json")
    )
    report = verify(root, request, offline=True)
    assert not any(
        name == preflight["module"]
        or name.startswith(preflight["module"] + ".")
        for name in sys.modules
    )
    assert {
        path.relative_to(root).as_posix(): sha(path)
        for path in root.rglob("*")
        if path.is_file()
    } == before
    report["original_capture_tree_unchanged"] = True
    create_once(root / "offline-report.json", report)
    return report


def method_refusal(args):
    """Fresh-process installed negative, not a fabricated version-only plugin."""
    from histdatacom.broker_plugin_capabilities import (
        BrokerCapabilityError,
        invoke_authorized_installed_broker_plugin,
    )

    root = args.output.absolute()
    root.mkdir(mode=0o700)
    installed_host(args.host_wheel_sha256)
    assert args.sdk_version == "1.1.0"
    inventory = discover_broker_plugins(sdk_version=args.sdk_version)
    driver = BrokerConformanceDriverV1.from_json(args.driver.read_text())
    request, permission = invocation(inventory, driver, args.sdk_version)
    registration = request.plan.candidate.registration
    installed_wheel(registration.distribution_name, args.plugin_wheel_sha256)
    expected = {
        "org.example.sdk11-missing": "unsupported_operation",
        "org.example.sdk11-wrong": "capability_violation",
        "org.example.sdk11-mismatch": "capability_violation",
    }[registration.plugin_id]
    with (
        provider_policy_scope(current_policy(request)),
        broker_permission_scope(authority(request, permission)),
    ):
        try:
            invoke_authorized_installed_broker_plugin(
                inventory,
                request.plan,
                authorize=lambda value: value == request.plan,
                provider_request=request,
            )
        except BrokerCapabilityError as error:
            assert error.reason.value == expected
        else:
            raise AssertionError(
                "SDK 1.1 method violation unexpectedly admitted"
            )
    report = {
        "plugin_id": registration.plugin_id,
        "reason": expected,
        "status": "refused_before_session",
        "nonclaim": NONCLAIM,
    }
    create_once(root / "method-refusal.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase", choices=("capture", "offline", "method-refusal")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--host-wheel-sha256", required=True)
    parser.add_argument("--plugin-wheel-sha256")
    parser.add_argument("--driver", type=Path)
    parser.add_argument(
        "--sdk-version", choices=("1.0.0", "1.1.0"), default="1.0.0"
    )
    args = parser.parse_args()
    if args.phase != "offline" and (
        args.driver is None or args.plugin_wheel_sha256 is None
    ):
        parser.error(
            "capture/refusal requires --driver and --plugin-wheel-sha256"
        )
    report = {
        "capture": capture,
        "offline": offline,
        "method-refusal": method_refusal,
    }[args.phase](args)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()

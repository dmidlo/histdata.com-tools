"""Generated-only operator example; public host APIs, never plugin internals.

Copy this file into either standalone project's tools directory. The plugin
runtime itself does not import it. Run from a clean installed host environment.
There is no platform fallback, provider transport, clock override or cleanup.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import stat
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from histdatacom.broker_plugin_capabilities import (
    BrokerAdmittedEventV1,
    BrokerCapabilityWorkflowV1,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_conformance import (
    BrokerConformanceDriverV1,
    inspect_broker_conformance_driver,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleReason,
    BrokerLifecycleRecordV1,
    BrokerLifecycleState,
    BrokerLifecycleTransitionV1,
    inspect_broker_lifecycle,
    replay_broker_lifecycle,
)
from histdatacom.broker_plugin_permissions import (
    BrokerPermissionAuthorityV1,
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionGrantV1,
    BrokerPermissionManifestV1,
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
    BrokerSecurityMode,
    BrokerSecurityPolicyV1,
    BrokerSecurityReceiptV1,
    BrokerTrustTier,
    read_security_receipt,
    run_secure_broker_plugin,
    verify_security_capture,
)
from histdatacom.broker_plugins import (
    BROKER_PLUGIN_SDK_VERSION,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
    BrokerDiagnosticSeverity,
    BrokerEventKind,
    BrokerReasonCode,
)

MAX_ANCHOR_BYTES = 2 * 1024 * 1024
LIFETIME_NS = 3_600_000_000_000
EMISSIONS = frozenset(("emit:health", "emit:quotes", "emit:sizes", "raw_payload:emit"))
NEEDED_EMISSIONS = ("emit:health", "emit:quotes")
CAPABILITIES = tuple(
    sorted(
        (
            "session.v1",
            "events.v1",
            "instruments.v1",
            "quotes.v1",
            "health.v1",
            "timestamps.broker-event.v1",
            "timestamps.receive.v1",
        )
    )
)
# HEALTH has no receive timestamp in this ABI. Require the declaration, but
# negotiate receive timestamps as optional and check the three actual quotes.
REQUIRED_CAPABILITIES = tuple(
    item for item in CAPABILITIES if item != "timestamps.receive.v1"
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
ANCHOR_FILES = frozenset(
    (
        "driver.json",
        "permissions.json",
        "request.json",
        "expected-seal.json",
        "expected-security.json",
        "capture-outcome.json",
    )
)
NONCLAIM = "generated_integrity_only_not_provider_authority_or_scientific_qualification"


class ExampleRefusal(ValueError):
    """Closed authored errors; never echo plugin/configuration exception text."""


def canonical_path(path: Path) -> Path:
    if path.name in ("", ".", ".."):
        raise ExampleRefusal("explicit_leaf_path_required")
    # Resolve the operator-owned parent only, never a potentially hostile leaf.
    return path.parent.resolve(strict=True) / path.name


def read_text(path: Path) -> str:
    path = canonical_path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(before.st_mode)
            or not 0 < before.st_size <= MAX_ANCHOR_BYTES
        ):
            raise ExampleRefusal("bounded_regular_artifact_required")
        payload = stream.read(MAX_ANCHOR_BYTES + 1)
        after = os.fstat(stream.fileno())
        fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if len(payload) != before.st_size or any(
            getattr(before, field) != getattr(after, field) for field in fields
        ):
            raise ExampleRefusal("artifact_changed_during_read")
    return payload.decode("ascii")


def create_once(path: Path, text: str) -> None:
    payload = text.encode("ascii")
    if not 0 < len(payload) <= MAX_ANCHOR_BYTES:
        raise ExampleRefusal("bounded_artifact_required")
    path = canonical_path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def output_paths(native: Path, anchor: Path, *, new: bool) -> tuple[Path, Path]:
    native, anchor = canonical_path(native), canonical_path(anchor)
    related = (native,) + tuple(
        native.with_name(native.name + suffix)
        for suffix in (
            "-host-health",
            "-provenance",
            "-security.json",
            "-security.json.provider-policy.json",
        )
    )
    for item in related:
        if item == anchor or item in anchor.parents or anchor in item.parents:
            raise ExampleRefusal("independent_anchor_location_required")
    if new:
        if any(path.exists() or path.is_symlink() for path in (*related, anchor)):
            raise ExampleRefusal("new_output_paths_required")
    elif native.is_symlink() or anchor.is_symlink() or not anchor.is_dir():
        raise ExampleRefusal("regular_native_and_anchor_directories_required")
    return native, anchor


def finite_configuration(driver: BrokerConformanceDriverV1) -> dict[str, str]:
    if driver.symbols != ("EURUSD",):
        raise ExampleRefusal("exact_generated_symbol_required")
    schema = BrokerConfigurationSchemaV1.from_json(driver.configuration_schema_json)
    # This portable example is intentionally narrower than the general driver ABI.
    if len(schema.fields) != 1 or (
        schema.fields[0].name != "mode"
        or schema.fields[0].value_type is not BrokerConfigurationType.STRING
        or schema.fields[0].secret
    ):
        raise ExampleRefusal("generated_mode_only_schema_required")
    scenarios = [item for item in driver.scenarios if item.scenario_id == "finite"]
    if len(scenarios) != 1 or scenarios[0].configuration_json != '{"mode":"finite"}':
        raise ExampleRefusal("exact_generated_finite_configuration_required")
    values = {"mode": "finite"}
    schema.validate_configuration(values)
    return values


def validate_emission_scope(driver, manifest, candidate) -> None:
    registration = candidate.registration
    if (
        driver.candidate_id != candidate.artifact_id
        or driver.permission_manifest_id != manifest.artifact_id
        or manifest.candidate_id != candidate.artifact_id
        or manifest.distribution_name != registration.distribution_name
        or manifest.distribution_version != registration.distribution_version
        or manifest.sdk_version != BROKER_PLUGIN_SDK_VERSION
        or driver.provider_id not in manifest.provider_ids
        or driver.provider_id not in registration.provider_ids
    ):
        raise ExampleRefusal("exact_candidate_permission_provider_binding_required")
    declared = set(manifest.required_atoms + manifest.optional_atoms)
    if (
        declared - EMISSIONS
        or manifest.endpoints
        or manifest.secret_profiles
        or manifest.caches
        or manifest.subprocess_mode != "none"
        or set(manifest.required_atoms) - set(NEEDED_EMISSIONS)
        or not set(NEEDED_EMISSIONS) <= declared
        or not set(CAPABILITIES) <= set(registration.capabilities)
    ):
        raise ExampleRefusal("generated_emission_only_permissions_required")
    finite_configuration(driver)


def make_request(inventory, driver, manifest):
    matches = [
        item for item in inventory.candidates if item.artifact_id == driver.candidate_id
    ]
    if len(matches) != 1:
        raise ExampleRefusal("exact_installed_candidate_required")
    candidate = matches[0]
    validate_emission_scope(driver, manifest, candidate)
    optional = tuple(
        sorted(
            {"timestamps.receive.v1"}
            | (
                {"instruments.price-increment.v1"}
                if "instruments.price-increment.v1"
                in candidate.registration.capabilities
                else set()
            )
        )
    )
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(OPERATIONS, REQUIRED_CAPABILITIES, optional),
        plugin_id=candidate.registration.plugin_id,
        version_constraint="==" + candidate.registration.plugin_version,
    )
    plan.require_admitted()
    if plan.candidate != candidate:
        raise ExampleRefusal("selected_candidate_changed")
    return BrokerSDKInvocationV1(
        plan,
        BrokerProviderConfigurationV1(
            driver.provider_id,
            "generated-finite-operator-example",
            driver.configuration_schema_json,
            '{"mode":"finite"}',
        ),
        BrokerProviderOutputContractV1("sdk-v1", ("health", "quote")),
    )


def validate_retained_scope(driver, manifest, request) -> None:
    validate_emission_scope(driver, manifest, request.plan.candidate)
    profile = request.configuration_profile
    expected_optional = tuple(
        sorted(
            {"timestamps.receive.v1"}
            | (
                {"instruments.price-increment.v1"}
                if "instruments.price-increment.v1"
                in request.plan.candidate.registration.capabilities
                else set()
            )
        )
    )
    if (
        request.plan.workflow
        != BrokerCapabilityWorkflowV1(
            OPERATIONS, REQUIRED_CAPABILITIES, expected_optional
        )
        or profile
        != BrokerProviderConfigurationV1(
            driver.provider_id,
            "generated-finite-operator-example",
            driver.configuration_schema_json,
            '{"mode":"finite"}',
        )
        or request.output_contract
        != BrokerProviderOutputContractV1("sdk-v1", ("health", "quote"))
    ):
        raise ExampleRefusal("retained_generated_scope_changed")


@dataclass
class PolicySource:
    context: BrokerPolicyContextV1

    def read_policy_context(self) -> BrokerPolicyContextV1:
        # A current operator-owned ledger, freshly restored at every host call.
        return BrokerPolicyContextV1.from_json(self.context.to_json())


@dataclass
class PermissionSource:
    context: BrokerPermissionContextV1

    def read_context(self) -> BrokerPermissionContextV1:
        return BrokerPermissionContextV1.from_json(self.context.to_json())


def policy_context(request, *, replay_only: bool, now_ns: int) -> BrokerPolicyContextV1:
    terms = b"Operator declaration for generated finite fixture only; no provider or credential rights."
    reference = BrokerPolicyReferenceV1(
        "generated-operator-terms",
        "finite-fixture-v1",
        hashlib.sha256(terms).hexdigest(),
        len(terms),
    )
    evidence = BrokerPolicyEvidenceV1(
        reference,
        request.configuration_profile.provider_id,
        "generated-finite-operator-example",
        "1",
        datetime.fromtimestamp(now_ns // 1_000_000_000, timezone.utc)
        .date()
        .isoformat(),
        "local-generated-operator",
        now_ns,
        BrokerPolicyEvidenceKind.DECLARED,
        "fixture:generated-finite",
    )
    operations = (
        {BrokerPolicyOperation.MATERIAL_USE}
        if replay_only
        else {
            BrokerPolicyOperation.INVOKE,
            BrokerPolicyOperation.CAPTURE,
            BrokerPolicyOperation.MATERIAL_USE,
            BrokerPolicyOperation.RETAIN_LOCAL,
        }
    )
    classes = {
        BrokerPolicyDataClass.HEALTH,
        BrokerPolicyDataClass.CONTENT_HASHES,
        BrokerPolicyDataClass.NORMALIZED_QUOTES,
    }
    rules = []
    for operation in sorted(BrokerPolicyOperation):
        for data_class in sorted(BrokerPolicyDataClass):
            allowed = operation in operations and data_class in classes
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
        now_ns,
        now_ns,
        now_ns + LIFETIME_NS,
    )
    operator = b"local-generated-operator-explicit-authorization"
    acknowledgement = BrokerPolicyAcknowledgementV1(
        policy.artifact_id,
        (evidence.artifact_id,),
        BrokerPolicyReferenceV1(
            "local-operator",
            "generated-finite-example",
            hashlib.sha256(operator).hexdigest(),
            len(operator),
        ),
        now_ns,
        now_ns + LIFETIME_NS,
    )
    return BrokerPolicyContextV1(
        (policy,),
        (evidence,),
        (acknowledgement,),
        (),
        (policy.artifact_id,),
        BrokerPolicyExecutionV1(False, "generated", "fixture", "fixture"),
    )


def permission_authority(request, manifest, *, now_ns: int):
    binding = BrokerPermissionBindingV1(
        request.plan.candidate.artifact_id,
        manifest.artifact_id,
        manifest.sdk_version,
        request.configuration_profile.provider_id,
        request.configuration_profile.artifact_id,
    )
    grant = BrokerPermissionGrantV1(
        binding,
        NEEDED_EMISSIONS,
        "local-generated-operator",
        now_ns,
        now_ns + LIFETIME_NS,
        secrets.token_hex(16),
    )
    source = PermissionSource(BrokerPermissionContextV1((grant,)))
    return BrokerPermissionAuthorityV1(manifest, binding, grant.artifact_id, source)


def bind_native_observations(
    manifest: BrokerLifecycleManifestV1,
    records: Iterable[BrokerLifecycleRecordV1],
) -> tuple[BrokerLifecycleRecordV1, ...]:
    """Bind observed canonical bytes to the opening manifest, not a later read.

    Public replay still performs its own current-rights/native validation, and
    the caller still requires pinned provenance and the original security
    receipt. This bounded snapshot grants no authority. Detaching every record
    before advancing the iterator prevents later mutation of yielded objects
    from changing the observations used for semantic checks.
    """
    if type(manifest) is not BrokerLifecycleManifestV1:
        raise ExampleRefusal("exact_native_manifest_required")
    manifest = BrokerLifecycleManifestV1.from_json(manifest.to_json())
    receipts = manifest.partitions + (
        () if manifest.partial_partition is None else (manifest.partial_partition,)
    )
    policy = manifest.header.policy
    if (
        manifest.completion is BrokerLifecycleCompletion.OPEN
        or len(receipts) > policy.max_partitions
        or sum(item.record_count for item in receipts) != manifest.appended_records
        or sum(item.byte_count for item in receipts) > policy.max_capture_bytes
    ):
        raise ExampleRefusal("native_observation_inventory_mismatch")
    iterator = iter(records)
    bound = []
    offset = total_events = 0
    run_id = manifest.header.artifact_id
    for receipt in receipts:
        if (
            receipt.first_sequence != offset
            or receipt.record_count > policy.partition_events
            or receipt.byte_count > policy.partition_bytes
        ):
            raise ExampleRefusal("native_observation_partition_limit")
        digest = hashlib.sha256()
        byte_count = event_count = 0
        for _ in range(receipt.record_count):
            try:
                record = next(iterator)
            except StopIteration:
                raise ExampleRefusal("native_observation_missing_record") from None
            if (
                type(record) is not BrokerLifecycleRecordV1
                or record.run_id != run_id
                or record.capture_sequence != offset
            ):
                raise ExampleRefusal("native_observation_identity_mismatch")
            canonical = record.to_json()
            detached = BrokerLifecycleRecordV1.from_json(canonical)
            encoded = (canonical + "\n").encode("ascii")
            byte_count += len(encoded)
            if byte_count > receipt.byte_count:
                raise ExampleRefusal("native_observation_partition_bytes_mismatch")
            digest.update(encoded)
            event_count += detached.kind == "event"
            bound.append(detached)
            offset += 1
        if (
            byte_count != receipt.byte_count
            or digest.hexdigest() != receipt.sha256
            or event_count != receipt.event_count
        ):
            raise ExampleRefusal("native_observation_partition_mismatch")
        total_events += event_count
    sentinel = object()
    if next(iterator, sentinel) is not sentinel:
        raise ExampleRefusal("native_observation_extra_record")
    if offset != manifest.appended_records or total_events != manifest.appended_events:
        raise ExampleRefusal("native_observation_totals_mismatch")
    return tuple(bound)


def verify_finite_native(
    native: Path, request, expected_seal, expected_security
) -> dict[str, object]:
    inspection = inspect_broker_lifecycle(native)
    if not inspection.complete:
        raise ExampleRefusal("complete_native_finite_capture_required")
    if type(inspection.manifest) is not BrokerLifecycleManifestV1:
        raise ExampleRefusal("exact_native_manifest_required")
    # Freeze the opening inventory before even calling the replay function.
    # A later successful reread of the original directory cannot authenticate
    # a substituted intermediate iterator or its generator-owned objects.
    manifest = BrokerLifecycleManifestV1.from_json(inspection.manifest.to_json())
    if (
        manifest.completion is not BrokerLifecycleCompletion.COMPLETE
        or manifest.state is not BrokerLifecycleState.STOPPED
        or not manifest.worker_reaped
        or manifest.forced_terminations
        or manifest.error_diagnostics
        or manifest.discarded_known
        or manifest.header.plan != request.plan
        or manifest.header.symbols != ("EURUSD",)
    ):
        raise ExampleRefusal("complete_native_finite_capture_required")
    records = bind_native_observations(
        manifest, replay_broker_lifecycle(native, provider_request=request)
    )
    events = tuple(
        BrokerAdmittedEventV1.from_json(record.payload_json).event
        for record in records
        if record.kind == "event"
    )
    transitions = tuple(
        BrokerLifecycleTransitionV1.from_json(record.payload_json)
        for record in records
        if record.kind == "transition"
    )
    if (
        tuple(event.kind for event in events)
        != (BrokerEventKind.HEALTH,) + (BrokerEventKind.QUOTE,) * 3
        or len(transitions) < 2
        or transitions[-2].reason is not BrokerLifecycleReason.EOF
        or transitions[-1].reason is not BrokerLifecycleReason.CLOSED
        or any(record.epoch != 0 for record in records)
    ):
        raise ExampleRefusal("exact_finite_native_stream_required")
    diagnostic = events[0].diagnostic
    if (
        diagnostic is None
        or diagnostic.code is not BrokerReasonCode.HEALTHY
        or diagnostic.severity is not BrokerDiagnosticSeverity.INFO
        or diagnostic.summary != BrokerReasonCode.HEALTHY.value
    ):
        raise ExampleRefusal("generated_healthy_info_prelude_required")
    for index, event in enumerate(events[1:], start=1):
        if (
            event.instrument != "EURUSD"
            or event.quote is None
            or event.quote.bid != "1.10000"
            or event.quote.ask != "1.20000"
            or event.quote.bid_size is not None
            or event.quote.ask_size is not None
            or event.quote.activity is not None
            or event.raw_provenance is not None
            or event.extensions
            or event.receive_time is None
            or event.source_time is None
            or event.source_time.timestamp_ns != index * 1_000_000_000
            or event.source_time.precision_ns != 1
            or event.source_time.semantics.value != "broker_event"
        ):
            raise ExampleRefusal("canonical_generated_quote_vectors_required")
    receipt = read_security_receipt(native.with_name(native.name + "-security.json"))
    if (
        receipt.to_json() != expected_security.to_json()
        or receipt.public_configuration_json
        != request.configuration_profile.public_configuration_json
    ):
        raise ExampleRefusal("original_security_receipt_required")
    require_provider_operation(
        BrokerSDKSecurityV1(request, receipt, manifest),
        BrokerPolicyOperation.MATERIAL_USE,
    )
    verify_security_capture(receipt, manifest)
    if (
        receipt.policy.mode is not BrokerSecurityMode.KERNEL_ISOLATED
        or receipt.policy.network is not BrokerNetworkMode.OFF
    ):
        raise ExampleRefusal("isolated_network_off_receipt_required")
    verified = read_lifecycle_capture_provenance(
        native,
        manifest,
        provider_request=request,
        expected_root=expected_seal,
    )
    if (
        verified.reason is not BrokerProvenanceVerificationReason.VERIFIED
        or not verified.anchored
        or not verified.complete
        or verified.seal != expected_seal
    ):
        raise ExampleRefusal("complete_independently_anchored_provenance_required")
    return {
        "status": "verified_generated_finite_capture",
        "manifest_id": manifest.artifact_id,
        "seal_id": expected_seal.artifact_id,
        "events": len(events),
        "nonclaim": NONCLAIM,
    }


def capture(driver_path: Path, native: Path, anchor: Path, *, authorized: bool):
    if authorized is not True:
        raise ExampleRefusal("generated_execution_authorization_required")
    native, anchor = output_paths(native, anchor, new=True)
    driver = BrokerConformanceDriverV1.from_json(read_text(driver_path))
    inventory = discover_broker_plugins()
    inspect_broker_conformance_driver(inventory, driver)
    matches = [
        item for item in inventory.candidates if item.artifact_id == driver.candidate_id
    ]
    if len(matches) != 1:
        raise ExampleRefusal("exact_installed_candidate_required")
    manifest = read_installed_broker_permissions(matches[0])
    request = make_request(inventory, driver, manifest)
    now = time.time_ns()
    policy = PolicySource(policy_context(request, replay_only=False, now_ns=now))
    authority = permission_authority(request, manifest, now_ns=now)
    security_policy = BrokerSecurityPolicyV1(
        request.plan.candidate.artifact_id,
        BrokerTrustTier.DEVELOPMENT,
        BrokerSecurityMode.KERNEL_ISOLATED,
        BrokerNetworkMode.OFF,
    )
    anchor.mkdir(mode=0o700)
    create_once(anchor / "driver.json", driver.to_json())
    create_once(anchor / "permissions.json", manifest.to_json())
    create_once(anchor / "request.json", request.to_json())
    result_received = False
    try:
        with provider_policy_scope(policy):
            resources = (
                BrokerPermissionResourcesV1(authority, provider_request=request)
                if (manifest.resource_abi == "host_resources_v1")
                else None
            )
            with broker_permission_scope(authority, resources=resources):
                result = run_secure_broker_plugin(
                    inventory,
                    request.plan,
                    security_policy,
                    finite_configuration(driver),
                    driver.symbols,
                    native,
                    authorize=lambda presented: type(presented)
                    is BrokerSecurityPolicyV1
                    and presented == security_policy,
                    provider_request=request,
                    protected_paths=(anchor,),
                    lifecycle_policy=BrokerLifecyclePolicyV1(
                        max_events=16,
                        startup_timeout_ms=30_000,
                        run_timeout_ms=120_000,
                        acknowledgement_timeout_ms=10_000,
                        shutdown_timeout_ms=100,
                        delivery_retries=0,
                        retry_delays_ms=(),
                    ),
                )
            result_received = True
            terminal = result.native.provenance.terminal
            if (
                terminal.native_manifest_id != result.native.manifest.artifact_id
                or terminal.native_manifest_sha256
                != hashlib.sha256(
                    result.native.manifest.to_json().encode("ascii")
                ).hexdigest()
            ):
                raise ExampleRefusal("returned_seal_manifest_binding_required")

            def retain_returned(name: str, text: str) -> None:
                # A scope is not a grant. Recheck the actual returned capture
                # and receipt immediately before every derived anchor write.
                require_provider_operation(
                    BrokerSDKSecurityV1(
                        request, result.security, result.native.manifest
                    ),
                    BrokerPolicyOperation.RETAIN_LOCAL,
                )
                create_once(anchor / name, text)

            # Preserve the original seal even for a returned partial run, if
            # current retention rights still permit writing this evidence.
            retain_returned("expected-seal.json", result.native.provenance.to_json())
            retain_returned("expected-security.json", result.security.to_json())
            outcome = {
                "returned": True,
                "completion": result.native.manifest.completion.value,
                "reason": result.native.reason.value,
                "worker_reaped": result.native.manifest.worker_reaped,
            }
            retain_returned(
                "capture-outcome.json",
                json.dumps(outcome, sort_keys=True, separators=(",", ":")),
            )
            return verify_finite_native(
                native, request, result.native.provenance, result.security
            )
    except Exception:
        if not result_received and not (anchor / "capture-outcome.json").exists():
            create_once(
                anchor / "capture-outcome.json",
                '{"returned":false,"status":"capture_or_anchor_write_failed"}',
            )
        raise


def replay(native: Path, anchor: Path, *, authorized: bool):
    if authorized is not True:
        raise ExampleRefusal("generated_material_use_authorization_required")
    native, anchor = output_paths(native, anchor, new=False)
    names = set()
    with os.scandir(anchor) as entries:
        for entry in entries:
            if len(names) >= len(ANCHOR_FILES) or not entry.is_file(
                follow_symlinks=False
            ):
                raise ExampleRefusal("exact_anchor_inventory_required")
            names.add(entry.name)
    if names != ANCHOR_FILES:
        raise ExampleRefusal("complete_external_anchor_required")
    driver = BrokerConformanceDriverV1.from_json(read_text(anchor / "driver.json"))
    manifest = BrokerPermissionManifestV1.from_json(
        read_text(anchor / "permissions.json")
    )
    request = BrokerSDKInvocationV1.from_json(read_text(anchor / "request.json"))
    expected = BrokerProvenanceSealV1.from_json(
        read_text(anchor / "expected-seal.json")
    )
    expected_security = BrokerSecurityReceiptV1.from_json(
        read_text(anchor / "expected-security.json")
    )
    validate_retained_scope(driver, manifest, request)
    # Do not discover, import, invoke, reinstall, or grant anything to the plugin.
    fresh = PolicySource(
        policy_context(request, replay_only=True, now_ns=time.time_ns())
    )
    with provider_policy_scope(fresh):
        return verify_finite_native(native, request, expected, expected_security)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("capture", "replay"):
        command = commands.add_parser(name)
        command.add_argument("--native-dir", required=True, type=Path)
        command.add_argument("--anchor-dir", required=True, type=Path)
        if name == "capture":
            command.add_argument("--driver", required=True, type=Path)
            command.add_argument(
                "--authorize-generated-execution", action="store_true", required=True
            )
        else:
            command.add_argument(
                "--authorize-generated-material-use", action="store_true", required=True
            )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = (
            capture(
                args.driver,
                args.native_dir,
                args.anchor_dir,
                authorized=args.authorize_generated_execution,
            )
            if args.command == "capture"
            else replay(
                args.native_dir,
                args.anchor_dir,
                authorized=args.authorize_generated_material_use,
            )
        )
    except Exception:  # noqa: BLE001 - never expose untrusted plugin error text.
        # A native error may derive from private plugin data. Keep it out of CLI logs.
        print(
            json.dumps(
                {
                    "status": "refused_or_incomplete",
                    "retained_outputs": "left_unchanged",
                    "nonclaim": NONCLAIM,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

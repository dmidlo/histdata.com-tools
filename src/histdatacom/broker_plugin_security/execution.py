"""Explicitly authorized security entry points; no implicit production use."""

from __future__ import annotations

import io
import logging
import re
import sys
import tempfile
import threading
from collections.abc import Callable, Mapping
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from histdatacom.broker_plugin_health import BrokerHostHealthPolicyV1
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionExecutionV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityPlanV1,
    invoke_authorized_installed_broker_plugin,
    verify_broker_capability_plan,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleIdentityV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleReason,
    BrokerLifecycleResultV1,
    BrokerLifecycleState,
    BrokerLifecycleTransitionV1,
    replay_broker_lifecycle,
    run_broker_plugin_lifecycle,
)
from histdatacom.broker_plugin_lifecycle.supervisor import (
    BrokerLifecycleExecutionHooks,
    Clock,
    _clock,
)
from histdatacom.broker_plugin_registry import BrokerPluginInventoryV1

from .contracts import (
    BrokerSecurityError,
    BrokerSecurityPolicyV1,
    BrokerSecurityReceiptV1,
    BrokerSoftwareProvenanceV1,
    BrokerTrustedSecurityReceiptV1,
    canonical_security_json,
    refuse,
)
from .contracts import (
    BrokerSecurityMode as Mode,
)
from .contracts import (
    BrokerSecurityReason as Reason,
)
from .isolation import prepare_kernel_launch, require_kernel_backend
from .provenance import installed_software_provenance
from .secrets import (
    BrokerPrivateMaterialGuard,
    BrokerSecretProvider,
    resolve_configuration,
)
from .storage import write_security_receipt

Authorization = Callable[[BrokerSecurityPolicyV1], bool]
_IN_PROCESS_LOCK = threading.Lock()


def _preflight(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    policy: BrokerSecurityPolicyV1,
    mode: Mode,
    authorize: Authorization,
    symbols: tuple[str, ...],
    provider_request: BrokerSDKInvocationV1,
) -> BrokerSDKInvocationV1:
    from histdatacom.broker_plugin_capabilities.execution import (
        _provider_call,
        _provider_request,
    )

    request: BrokerSDKInvocationV1 = _provider_request(plan, provider_request)
    _provider_call(request, capture=False)
    try:
        BrokerSecurityPolicyV1.from_json(policy.to_json())
        if (
            policy.mode is not mode
            or policy.candidate_id != plan.candidate.artifact_id
        ):
            refuse()
        # Old raw-secret configuration and ambient loopback grants remain
        # readable V1 evidence, but are not the new host-resource extension.
        # Approved network/authentication must go through the permission scope.
        if policy.secret_fields or policy.loopback_ports:
            refuse(Reason.RESOURCE)
        verify_broker_capability_plan(plan, inventory)
        plan.require_admitted()
        required = {"configuration_schema", "open_session", "iter_events"}
        if not required.issubset(plan.workflow.operations):
            refuse()
        if (
            type(symbols) is not tuple
            or len(symbols) > 128
            or any(
                type(item) is not str
                or re.fullmatch(r"[A-Z]{6}", item) is None
                or item[:3] == item[3:]
                for item in symbols
            )
            or symbols != tuple(sorted(set(symbols)))
            or (
                symbols
                and not {"instruments", "subscribe", "unsubscribe"}.issubset(
                    plan.workflow.operations
                )
            )
        ):
            refuse()
        if mode is Mode.KERNEL_ISOLATED:
            require_kernel_backend()
    except BrokerSecurityError:
        raise
    except BaseException:  # noqa: BLE001
        # Sanitize untrusted policy validation.
        refuse()
    try:
        if authorize(policy) is not True:
            refuse(Reason.AUTHORIZATION)
    except BaseException:  # noqa: BLE001 - sanitize operator callback errors.
        refuse(Reason.AUTHORIZATION)
    return request


def _private_inputs(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    policy: BrokerSecurityPolicyV1,
    public: Mapping[str, object],
    handles: Mapping[str, str],
    provider: BrokerSecretProvider | None,
    private_identifiers: tuple[str, ...],
    attestation: bytes | None,
    provider_request: BrokerSDKInvocationV1,
) -> tuple[
    dict[str, object],
    BrokerPrivateMaterialGuard,
    BrokerSoftwareProvenanceV1,
    str,
]:
    from histdatacom.broker_plugin_capabilities.execution import _provider_call
    from histdatacom.broker_plugin_policy.scope import (
        BrokerPolicyError,
        read_current_provider_policy_context,
    )

    _provider_call(provider_request, capture=False)
    profile = provider_request.configuration_profile
    if (
        canonical_security_json(dict(public))
        != profile.public_configuration_json
        or set(policy.secret_fields) != set(profile.private_field_names)
        or set(handles) != set(profile.private_field_names)
    ):
        raise BrokerPolicyError("reviewed_security_configuration_mismatch")
    configuration, guard = resolve_configuration(
        public, handles, policy.secret_fields, provider, private_identifiers
    )
    try:
        profile.verify_configuration(configuration)
        public_json = canonical_security_json(dict(public))
        # Scan the FULL inventory and plan, not only selected registration.
        for text in (
            inventory.to_json(),
            plan.to_json(),
            policy.to_json(),
            public_json,
            provider_request.to_json(),
            read_current_provider_policy_context().to_json(),
        ):
            guard.check(text)
        if attestation is not None:
            guard.check(attestation.decode("utf-8", errors="replace"))
        software = installed_software_provenance(
            plan.candidate, attestation=attestation
        )
        guard.check(software.to_json())
        return configuration, guard, software, public_json
    except BaseException:
        configuration.clear()
        raise


@dataclass(frozen=True, slots=True)
class BrokerSecureLifecycleResultV1:
    native: BrokerLifecycleResultV1
    security: BrokerSecurityReceiptV1
    receipt_path: Path


@dataclass(frozen=True, slots=True)
class BrokerTrustedSecurityResultV2:
    """Native V1 receipt plus exact historical permission admission evidence."""

    receipt: BrokerTrustedSecurityReceiptV1
    permissions: BrokerPermissionExecutionV1

    def __post_init__(self) -> None:
        from histdatacom.broker_plugin_permissions import (
            BrokerPermissionExecutionV1,
            verify_permission_execution,
        )
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKInvocationV1,
        )

        if (
            type(self.receipt) is not BrokerTrustedSecurityReceiptV1
            or type(self.permissions) is not BrokerPermissionExecutionV1
        ):
            raise ValueError("exact trusted security evidence required")
        verify_permission_execution(
            self.permissions,
            BrokerSDKInvocationV1.from_json(self.permissions.invocation_json),
            self.receipt,
        )


def _verify_configuration_classification(
    native: BrokerLifecycleResultV1,
    configuration: Mapping[str, object],
    public: Mapping[str, object],
    policy: BrokerSecurityPolicyV1,
    provider_request: BrokerSDKInvocationV1,
) -> None:
    """Never persist caller-labeled public data without every epoch's schema."""
    started: set[int] = set()
    verified: set[int] = set()
    for record in replay_broker_lifecycle(
        native.directory, provider_request=provider_request
    ):
        if record.kind == "transition":
            transition = BrokerLifecycleTransitionV1.from_json(
                record.payload_json
            )
            if transition.current is BrokerLifecycleState.STARTING:
                started.add(record.epoch)
        elif record.kind == "identity":
            identity = BrokerLifecycleIdentityV1.from_json(record.payload_json)
            declared = {
                field.name
                for field in identity.configuration_schema.fields
                if field.secret
            }
            if declared.intersection(public) or declared.intersection(
                configuration
            ) != set(policy.secret_fields):
                refuse(Reason.INTEGRITY)
            try:
                identity.configuration_schema.validate_configuration(
                    configuration
                )
            except (TypeError, ValueError):
                refuse(Reason.INTEGRITY)
            verified.add(record.epoch)
    if not started or verified != started:
        refuse(Reason.INTEGRITY)


def run_secure_broker_plugin(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    policy: BrokerSecurityPolicyV1,
    public_configuration: Mapping[str, object],
    symbols: tuple[str, ...],
    output_directory: Path,
    *,
    authorize: Authorization,
    provider_request: BrokerSDKInvocationV1,
    secret_handles: Mapping[str, str] | None = None,
    secret_provider: BrokerSecretProvider | None = None,
    private_identifiers: tuple[str, ...] = (),
    protected_paths: tuple[Path, ...] = (),
    lifecycle_policy: BrokerLifecyclePolicyV1 | None = None,
    worker_python: str = sys.executable,
    run_nonce: str | None = None,
    cancellation: Callable[[], bool] = lambda: False,
    clock: Clock = _clock,
    attestation: bytes | None = None,
    health_policy: BrokerHostHealthPolicyV1 | None = None,
) -> BrokerSecureLifecycleResultV1:
    """Kernel-isolated native capture, with known-private-material refusal.

    Unsupported enforcement refuses before resolver/output/plugin invocation.
    Direct worker networking is off. Explicit permission-scoped host resources
    mediate bounded transport without exporting resolved authentication values.
    """
    from histdatacom.broker_plugin_policy.scope import BrokerPolicyError

    request = _preflight(
        inventory,
        plan,
        policy,
        Mode.KERNEL_ISOLATED,
        authorize,
        symbols,
        provider_request,
    )
    configuration: dict[str, object] = {}
    try:
        with tempfile.TemporaryDirectory(
            prefix="broker-security-"
        ) as directory:
            launch = prepare_kernel_launch(
                worker_python,
                Path(__file__).resolve().parents[2],
                Path(directory).resolve(),
                (output_directory, *protected_paths),
                (),
            )
            configuration, guard, software, public_json = _private_inputs(
                inventory,
                plan,
                policy,
                public_configuration,
                secret_handles or {},
                secret_provider,
                private_identifiers,
                attestation,
                request,
            )
            native = run_broker_plugin_lifecycle(
                inventory,
                plan,
                configuration,
                symbols,
                output_directory,
                authorize=lambda _: True,
                provider_request=request,
                policy=lifecycle_policy,
                health_policy=health_policy,
                worker_python=worker_python,
                run_nonce=run_nonce,
                cancellation=cancellation,
                clock=clock,
                execution_hooks=BrokerLifecycleExecutionHooks(
                    launch.command_prefix,
                    launch.environment,
                    launch.working_directory,
                    policy.secret_fields,
                    guard.check,
                    bootstrap_source=launch.bootstrap_source,
                ),
            )
            if guard.refused:
                refuse(Reason.SECRET)
            if native.reason is BrokerLifecycleReason.INTEGRITY:
                refuse(Reason.INTEGRITY)
            _verify_configuration_classification(
                native, configuration, public_configuration, policy, request
            )
            guard.check(native.manifest.to_json())
            receipt = BrokerSecurityReceiptV1(
                policy,
                software,
                native.manifest.artifact_id,
                public_json,
                "macos-seatbelt-v1",
            )
            guard.check(receipt.to_json())
            receipt_path = output_directory.with_name(
                output_directory.name + "-security.json"
            )
            write_security_receipt(
                receipt,
                receipt_path,
                provider_request=request,
                manifest=native.manifest,
                before_persist=guard.check,
            )
            return BrokerSecureLifecycleResultV1(native, receipt, receipt_path)
    except (BrokerSecurityError, BrokerPolicyError):
        raise
    except BaseException:  # noqa: BLE001 - never expose private plugin errors.
        refuse(Reason.EXECUTION)
    finally:
        configuration.clear()


class _Discard(io.TextIOBase):
    def __init__(self) -> None:
        self.count = 0

    def write(self, text: str) -> int:
        self.count += len(text)
        if self.count > 65_536:
            refuse(Reason.RESOURCE)
        return len(text)


def run_trusted_broker_plugin(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    policy: BrokerSecurityPolicyV1,
    public_configuration: Mapping[str, object],
    symbols: tuple[str, ...],
    *,
    authorize: Authorization,
    provider_request: BrokerSDKInvocationV1,
    secret_handles: Mapping[str, str] | None = None,
    secret_provider: BrokerSecretProvider | None = None,
    private_identifiers: tuple[str, ...] = (),
    max_events: int = 128,
) -> BrokerTrustedSecurityResultV2:
    """Finite synchronous native events; trusted code, no hard cancellation.

    Python stdout/stderr and normal logging are discarded during invocation.
    Raw OS descriptors, other threads and native code are NOT contained here.
    Call only in a caller-owned quiescent process; strict isolation uses the
    separate secure lifecycle entry point. No arbitrary factory injection.
    """
    from histdatacom.broker_plugin_permissions import build_permission_execution
    from histdatacom.broker_plugin_permissions.scope import (
        check_permission_public_output,
        current_permission_authority,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKSecurityV1
    from histdatacom.broker_plugin_policy.contracts import BrokerPolicyOperation
    from histdatacom.broker_plugin_policy.scope import (
        BrokerPolicyError,
        require_provider_operation,
    )

    request = _preflight(
        inventory,
        plan,
        policy,
        Mode.TRUSTED_IN_PROCESS,
        authorize,
        symbols,
        provider_request,
    )
    if type(max_events) is not int or not 1 <= max_events <= 128:
        refuse(Reason.RESOURCE)
    configuration: dict[str, object] = {}
    try:
        configuration, guard, software, public_json = _private_inputs(
            inventory,
            plan,
            policy,
            public_configuration,
            secret_handles or {},
            secret_provider,
            private_identifiers,
            None,
            request,
        )
        with (
            _IN_PROCESS_LOCK,
            redirect_stdout(_Discard()),
            redirect_stderr(_Discard()),
        ):
            logging_level = logging.root.manager.disable
            logging.disable(sys.maxsize)
            plugin = None
            try:
                plugin = invoke_authorized_installed_broker_plugin(
                    inventory,
                    plan,
                    authorize=lambda _: True,
                    provider_request=request,
                )
                metadata = plugin.metadata
                schema = plugin.configuration_schema
                guard.check(metadata.to_json())
                guard.check(schema.to_json())
                if {
                    item.name for item in schema.fields if item.secret
                }.intersection(configuration) != set(policy.secret_fields):
                    refuse(Reason.SECRET)
                schema.validate_configuration(configuration)
                session = plugin.open_session(configuration)
                guard.check(session.to_json())
                if symbols:
                    for instrument in plugin.instruments():
                        guard.check(instrument.to_json())
                    plugin.subscribe(symbols)
                events: list[str] = []
                for event in plugin.iter_events(max_events=max_events):
                    text = event.to_json()
                    guard.check(text)
                    events.append(text)
                if symbols:
                    plugin.unsubscribe(symbols)
                receipt = BrokerTrustedSecurityReceiptV1(
                    policy,
                    software,
                    plan.to_json(),
                    metadata.to_json(),
                    plugin.binding.to_json(),
                    session.to_json(),
                    tuple(events),
                    public_json,
                )
                guard.check(receipt.to_json())
                require_provider_operation(
                    BrokerSDKSecurityV1(request, receipt),
                    BrokerPolicyOperation.MATERIAL_USE,
                )
                check_permission_public_output(receipt.to_json())
                permissions = build_permission_execution(
                    request, receipt, current_permission_authority()
                )
                guard.check(permissions.to_json())
                check_permission_public_output(permissions.to_json())
                return BrokerTrustedSecurityResultV2(receipt, permissions)
            finally:
                try:
                    if plugin is not None:
                        plugin.close_session()
                finally:
                    logging.disable(logging_level)
    except (BrokerSecurityError, BrokerPolicyError):
        raise
    except BaseException:  # noqa: BLE001 - never expose private plugin errors.
        refuse(Reason.EXECUTION)
    finally:
        configuration.clear()


def verify_security_capture(
    receipt: BrokerSecurityReceiptV1, manifest: BrokerLifecycleManifestV1
) -> None:
    """Bind to an authoritative manifest; partition replay is still separate."""
    try:
        BrokerSecurityReceiptV1.from_json(receipt.to_json())
        BrokerLifecycleManifestV1.from_json(manifest.to_json())
        if (
            receipt.native_manifest_id != manifest.artifact_id
            or receipt.policy.candidate_id
            != manifest.header.plan.candidate.artifact_id
        ):
            refuse(Reason.INTEGRITY)
        candidate = manifest.header.plan.candidate
        registration = candidate.registration
        if (
            receipt.software.registration_sha256
            != candidate.registration_sha256
            or receipt.software.implementation_sha256
            != candidate.implementation_sha256
            or receipt.software.distribution_name
            != registration.distribution_name
            or receipt.software.distribution_version
            != registration.distribution_version
            or receipt.software.sdk_version
            != manifest.header.inventory.sdk_version
        ):
            refuse(Reason.INTEGRITY)
    except BaseException:  # noqa: BLE001 - closed receipt integrity boundary.
        refuse(Reason.INTEGRITY)

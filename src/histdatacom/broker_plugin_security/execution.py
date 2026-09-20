"""Explicitly authorized security entry points; no implicit production use."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
import io
import logging
from pathlib import Path
import re
import sys
import tempfile
import threading

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityPlanV1,
    invoke_authorized_installed_broker_plugin,
    verify_broker_capability_plan,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleResultV1,
    BrokerLifecycleReason,
    BrokerLifecycleIdentityV1,
    BrokerLifecycleTransitionV1,
    BrokerLifecycleState,
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
    BrokerSecurityMode as Mode,
    BrokerSecurityPolicyV1,
    BrokerSecurityReason as Reason,
    BrokerSecurityReceiptV1,
    BrokerSoftwareProvenanceV1,
    BrokerTrustedSecurityReceiptV1,
    canonical_security_json,
    refuse,
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
) -> None:
    try:
        BrokerSecurityPolicyV1.from_json(policy.to_json())
        if (
            policy.mode is not mode
            or policy.candidate_id != plan.candidate.artifact_id
        ):
            refuse()
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
    except BaseException:
        refuse()
    try:
        if authorize(policy) is not True:
            refuse(Reason.AUTHORIZATION)
    except BaseException:
        refuse(Reason.AUTHORIZATION)


def _private_inputs(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    policy: BrokerSecurityPolicyV1,
    public: Mapping[str, object],
    handles: Mapping[str, str],
    provider: BrokerSecretProvider | None,
    private_identifiers: tuple[str, ...],
    attestation: bytes | None,
) -> tuple[
    dict[str, object],
    BrokerPrivateMaterialGuard,
    BrokerSoftwareProvenanceV1,
    str,
]:
    configuration, guard = resolve_configuration(
        public, handles, policy.secret_fields, provider, private_identifiers
    )
    try:
        public_json = canonical_security_json(dict(public))
        # Scan the FULL inventory and plan, not only selected registration.
        for text in (
            inventory.to_json(),
            plan.to_json(),
            policy.to_json(),
            public_json,
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


def _verify_configuration_classification(
    native: BrokerLifecycleResultV1,
    configuration: Mapping[str, object],
    public: Mapping[str, object],
    policy: BrokerSecurityPolicyV1,
) -> None:
    """Never persist caller-labeled public data without every epoch's schema."""
    started: set[int] = set()
    verified: set[int] = set()
    for record in replay_broker_lifecycle(native.directory):
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
            identity.configuration_schema.validate_configuration(configuration)
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
) -> BrokerSecureLifecycleResultV1:
    """Kernel-isolated native capture, with known-private-material refusal.

    Unsupported enforcement refuses before resolver/output/plugin invocation.
    Network is off or exact caller-owned loopback ports. No provider TLS or
    proxy is provisioned, checked or implicitly trusted by this function.
    """
    _preflight(
        inventory, plan, policy, Mode.KERNEL_ISOLATED, authorize, symbols
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
                policy.loopback_ports,
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
            )
            native = run_broker_plugin_lifecycle(
                inventory,
                plan,
                configuration,
                symbols,
                output_directory,
                authorize=lambda _: True,
                policy=lifecycle_policy,
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
                ),
            )
            if guard.refused:
                refuse(Reason.SECRET)
            if native.reason is BrokerLifecycleReason.INTEGRITY:
                refuse(Reason.INTEGRITY)
            _verify_configuration_classification(
                native, configuration, public_configuration, policy
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
            write_security_receipt(receipt, receipt_path)
            return BrokerSecureLifecycleResultV1(native, receipt, receipt_path)
    except BrokerSecurityError:
        raise
    except BaseException:
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
    secret_handles: Mapping[str, str] | None = None,
    secret_provider: BrokerSecretProvider | None = None,
    private_identifiers: tuple[str, ...] = (),
    max_events: int = 128,
) -> BrokerTrustedSecurityReceiptV1:
    """Finite synchronous native events; trusted code, no hard cancellation.

    Python stdout/stderr and normal logging are discarded during invocation.
    Raw OS descriptors, other threads and native code are NOT contained here.
    Call only in a caller-owned quiescent process; strict isolation uses the
    separate secure lifecycle entry point. No arbitrary factory injection.
    """
    _preflight(
        inventory, plan, policy, Mode.TRUSTED_IN_PROCESS, authorize, symbols
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
                    inventory, plan, authorize=lambda _: True
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
                return receipt
            finally:
                try:
                    if plugin is not None:
                        plugin.close_session()
                finally:
                    logging.disable(logging_level)
    except BrokerSecurityError:
        raise
    except BaseException:
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
    except BaseException:
        refuse(Reason.INTEGRITY)

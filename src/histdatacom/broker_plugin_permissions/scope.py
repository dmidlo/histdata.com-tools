"""Host-selected permission authority at actual native invocation boundaries."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .decisions import BrokerPermissionAuthorityV1, BrokerPermissionError

if TYPE_CHECKING:
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugins import BrokerEventV1, BrokerPluginMetadataV1
    from histdatacom.broker_plugins.resources import BrokerHostResourcesV1


@dataclass(frozen=True, slots=True)
class _Scope:
    authority: BrokerPermissionAuthorityV1
    resources: BrokerHostResourcesV1 | None
    pid: int


_CURRENT: ContextVar[_Scope | None] = ContextVar(
    "broker_permission_scope", default=None
)


def _require_resource_binding(
    authority: BrokerPermissionAuthorityV1, resources: BrokerHostResourcesV1
) -> None:
    """Do not attach a broader resource authority to a narrower native proof."""
    from .resources import BrokerPermissionResourcesV1
    from .worker import (
        PermissionChannel,
        WorkerHostResources,
        WorkerPermissionSource,
    )

    if type(resources) is BrokerPermissionResourcesV1:
        if resources.authority is authority:
            return
    elif type(resources) is WorkerHostResources:
        source = authority._source
        if (
            type(source) is WorkerPermissionSource
            and type(resources.channel) is PermissionChannel
            and source.channel is resources.channel
        ):
            return
    raise BrokerPermissionError("exact_bound_host_resources_required")


@contextmanager
def broker_permission_scope(
    authority: BrokerPermissionAuthorityV1,
    *,
    resources: BrokerHostResourcesV1 | None = None,
) -> Iterator[None]:
    """Pin the host choice; every effect still rereads its current grant source.

    This is not protection from a hostile same-process Python caller. Isolated
    workers receive a parent-mediated resource proxy and a fresh-ledger channel.
    Nested scopes and inherited scopes after fork cannot replace authority.
    """
    if (
        _CURRENT.get() is not None
        or type(authority) is not BrokerPermissionAuthorityV1
    ):
        raise BrokerPermissionError(
            "exact_unnested_permission_authority_required"
        )
    authority.require_admission()
    if (
        authority.manifest.resource_abi == "host_resources_v1"
        and resources is None
    ):
        raise BrokerPermissionError("declared_resource_abi_requires_resources")
    if authority.manifest.resource_abi == "none" and resources is not None:
        raise BrokerPermissionError(
            "resource_free_factory_cannot_receive_resources"
        )
    if resources is not None:
        _require_resource_binding(authority, resources)
    token = _CURRENT.set(_Scope(authority, resources, os.getpid()))
    try:
        yield
    finally:
        _CURRENT.reset(token)


def current_permission_authority() -> BrokerPermissionAuthorityV1:
    state = _CURRENT.get()
    if state is None or state.pid != os.getpid():
        raise BrokerPermissionError("current_process_permission_scope_required")
    return state.authority


def current_host_resources() -> BrokerHostResourcesV1:
    authority = current_permission_authority()
    authority.require_admission()
    state = _CURRENT.get()
    assert state is not None
    if state.resources is None:
        raise BrokerPermissionError("host_resources_not_declared")
    _require_resource_binding(authority, state.resources)
    return state.resources


def require_native_permissions(
    request: BrokerSDKInvocationV1, *, installed: bool = False
) -> BrokerPermissionAuthorityV1:
    """Compare actual SDK/provider/configuration identities, never caller labels."""
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugins import BROKER_PLUGIN_SDK_VERSION

    if type(request) is not BrokerSDKInvocationV1:
        raise BrokerPermissionError("exact_native_sdk_invocation_required")
    request = BrokerSDKInvocationV1.from_json(request.to_json())
    authority = current_permission_authority()
    manifest, binding = authority.manifest, authority.binding
    candidate = request.plan.candidate
    registration = candidate.registration
    profile = request.configuration_profile
    if (
        binding.candidate_id != candidate.artifact_id
        or binding.manifest_id != manifest.artifact_id
        or binding.provider_id != profile.provider_id
        or binding.configuration_id != profile.artifact_id
        or manifest.candidate_id != candidate.artifact_id
        or manifest.distribution_name != registration.distribution_name
        or manifest.distribution_version != registration.distribution_version
        or manifest.sdk_version != binding.sdk_version
        or manifest.sdk_version != BROKER_PLUGIN_SDK_VERSION
        or profile.provider_id not in manifest.provider_ids
    ):
        raise BrokerPermissionError("native_permission_binding_mismatch")
    # Required secrets in the old primitive configuration are not opaque
    # resource handles. Do not silently relabel plaintext export as permission.
    if profile.private_field_names:
        raise BrokerPermissionError("opaque_host_secret_profiles_required")
    if installed:
        from .discovery import read_installed_broker_permissions

        actual = read_installed_broker_permissions(candidate)
        if actual.to_json() != manifest.to_json():
            raise BrokerPermissionError(
                "installed_permission_manifest_mismatch"
            )
    decision = authority.require_admission()
    if "subprocess:requested" in decision.effective_atoms:
        raise BrokerPermissionError("isolated_subprocess_resource_unsupported")
    return authority


def require_event_permissions(event: BrokerEventV1) -> None:
    """Classify the actual SDK event; optional denial never fabricates fields."""
    from histdatacom.broker_plugins import BrokerEventKind, BrokerEventV1

    if type(event) is not BrokerEventV1:
        raise BrokerPermissionError("exact_sdk_event_required")
    checked = BrokerEventV1.from_json(event.to_json())
    authority = current_permission_authority()
    authority.require(
        "emit:quotes"
        if checked.kind is BrokerEventKind.QUOTE
        else "emit:health"
    )
    if checked.quote is not None and (
        checked.quote.bid_size is not None
        or checked.quote.ask_size is not None
        or checked.quote.activity is not None
    ):
        authority.require("emit:sizes")
    if checked.raw_provenance is not None or checked.extensions:
        authority.require("raw_payload:emit")


def require_metadata_permissions(metadata: BrokerPluginMetadataV1) -> None:
    from histdatacom.broker_plugins import BrokerPluginMetadataV1

    if type(metadata) is not BrokerPluginMetadataV1:
        raise BrokerPermissionError("exact_sdk_metadata_required")
    checked = BrokerPluginMetadataV1.from_json(metadata.to_json())
    if checked.extensions:
        current_permission_authority().require("raw_payload:emit")


def check_permission_public_output(text: str) -> None:
    """Check known host-resolved secrets before any native public persistence."""
    from .resources import BrokerPermissionResourcesV1

    state = _CURRENT.get()
    if state is None or state.pid != os.getpid():
        raise BrokerPermissionError("current_process_permission_scope_required")
    if state.resources is not None:
        _require_resource_binding(state.authority, state.resources)
    if type(state.resources) is BrokerPermissionResourcesV1:
        state.resources.check_public_text(text)

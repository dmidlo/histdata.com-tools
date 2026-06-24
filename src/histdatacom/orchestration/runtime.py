"""Public orchestration runtime path and port policy."""

from __future__ import annotations

from histdatacom.sidecar.runtime import (
    DEFAULT_BIND_IP,
    DEFAULT_PORT_SCAN_LIMIT,
    DEFAULT_PORT_WINDOW,
    DEFAULT_TEMPORAL_PORT_BASE,
    DEFAULT_TEMPORAL_UI_PORT_OFFSET,
    SIDECAR_IP_ENV,
    SIDECAR_PORT_ENV,
    SIDECAR_RUNTIME_HOME_ENV,
    SIDECAR_UI_PORT_ENV,
    SIDECAR_WORKSPACE_ENV,
    PortAllocationError,
    PortAvailabilityProbe,
    SidecarPaths as RuntimePaths,
    SidecarPorts as RuntimePorts,
    SidecarRuntimePolicy as RuntimePolicy,
    build_sidecar_runtime_policy,
    default_sidecar_runtime_home,
    default_sidecar_state_dir,
    default_sidecar_workspace,
    is_port_available,
)

RUNTIME_HOME_ENV = SIDECAR_RUNTIME_HOME_ENV
WORKSPACE_ENV = SIDECAR_WORKSPACE_ENV
BIND_IP_ENV = SIDECAR_IP_ENV
TEMPORAL_PORT_ENV = SIDECAR_PORT_ENV
TEMPORAL_UI_PORT_ENV = SIDECAR_UI_PORT_ENV

build_runtime_policy = build_sidecar_runtime_policy
default_runtime_home = default_sidecar_runtime_home
default_state_dir = default_sidecar_state_dir
default_workspace = default_sidecar_workspace

__all__ = [
    "BIND_IP_ENV",
    "DEFAULT_BIND_IP",
    "DEFAULT_PORT_SCAN_LIMIT",
    "DEFAULT_PORT_WINDOW",
    "DEFAULT_TEMPORAL_PORT_BASE",
    "DEFAULT_TEMPORAL_UI_PORT_OFFSET",
    "PortAllocationError",
    "PortAvailabilityProbe",
    "RUNTIME_HOME_ENV",
    "RuntimePaths",
    "RuntimePolicy",
    "RuntimePorts",
    "TEMPORAL_PORT_ENV",
    "TEMPORAL_UI_PORT_ENV",
    "WORKSPACE_ENV",
    "build_runtime_policy",
    "default_runtime_home",
    "default_state_dir",
    "default_workspace",
    "is_port_available",
]

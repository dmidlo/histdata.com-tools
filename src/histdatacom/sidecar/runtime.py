"""Runtime path and port policy for the local Temporal runtime."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import socket
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Mapping

RUNTIME_HOME_ENV = "HISTDATACOM_RUNTIME_HOME"
WORKSPACE_ENV = "HISTDATACOM_RUNTIME_WORKSPACE"
BIND_IP_ENV = "HISTDATACOM_RUNTIME_IP"
TEMPORAL_PORT_ENV = "HISTDATACOM_RUNTIME_PORT"
TEMPORAL_UI_PORT_ENV = "HISTDATACOM_RUNTIME_UI_PORT"

DEFAULT_BIND_IP = "127.0.0.1"
DEFAULT_TEMPORAL_PORT_BASE = 17233
DEFAULT_TEMPORAL_UI_PORT_OFFSET = 1000
DEFAULT_PORT_WINDOW = 2000
DEFAULT_PORT_SCAN_LIMIT = 64

PortAvailabilityProbe = Callable[[str, int], bool]


class PortAllocationError(RuntimeError):
    """Raised when runtime port allocation cannot find usable ports."""


@dataclass(frozen=True, slots=True)
class SidecarPaths:
    """Filesystem paths used by the runtime and supervisor."""

    runtime_dir: Path
    state_dir: Path
    logs_dir: Path
    sqlite_dir: Path
    manifests_dir: Path
    pid_file: Path
    lock_file: Path
    server_log: Path
    worker_log: Path
    sqlite_db: Path
    runtime_manifest: Path

    @classmethod
    def from_runtime_dir(cls, runtime_dir: Path | str) -> "SidecarPaths":
        """Create runtime paths under a workspace runtime directory."""
        root = Path(runtime_dir).expanduser()
        state_dir = root / "state"
        logs_dir = root / "logs"
        sqlite_dir = root / "sqlite"
        manifests_dir = root / "manifests"
        return cls(
            runtime_dir=root,
            state_dir=state_dir,
            logs_dir=logs_dir,
            sqlite_dir=sqlite_dir,
            manifests_dir=manifests_dir,
            pid_file=state_dir / "runtime.pid.json",
            lock_file=state_dir / "runtime.lock",
            server_log=logs_dir / "temporal-server.log",
            worker_log=logs_dir / "temporal-worker.log",
            sqlite_db=sqlite_dir / "temporal.db",
            runtime_manifest=manifests_dir / "runtime-policy.json",
        )

    @classmethod
    def from_state_dir(cls, state_dir: Path | str) -> "SidecarPaths":
        """Create runtime paths from an explicit state directory override."""
        state_root = Path(state_dir).expanduser()
        runtime_dir = (
            state_root.parent if state_root.name == "state" else state_root
        )
        logs_dir = runtime_dir / "logs"
        sqlite_dir = runtime_dir / "sqlite"
        manifests_dir = runtime_dir / "manifests"
        return cls(
            runtime_dir=runtime_dir,
            state_dir=state_root,
            logs_dir=logs_dir,
            sqlite_dir=sqlite_dir,
            manifests_dir=manifests_dir,
            pid_file=state_root / "runtime.pid.json",
            lock_file=state_root / "runtime.lock",
            server_log=logs_dir / "temporal-server.log",
            worker_log=logs_dir / "temporal-worker.log",
            sqlite_db=sqlite_dir / "temporal.db",
            runtime_manifest=manifests_dir / "runtime-policy.json",
        )

    def ensure_directories(self) -> None:
        """Create runtime directories."""
        for directory in (
            self.runtime_dir,
            self.state_dir,
            self.logs_dir,
            self.sqlite_dir,
            self.manifests_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, str]:
        """Return a JSON-compatible path mapping."""
        return {
            "runtime_dir": str(self.runtime_dir),
            "state_dir": str(self.state_dir),
            "logs_dir": str(self.logs_dir),
            "sqlite_dir": str(self.sqlite_dir),
            "manifests_dir": str(self.manifests_dir),
            "pid_file": str(self.pid_file),
            "lock_file": str(self.lock_file),
            "server_log": str(self.server_log),
            "worker_log": str(self.worker_log),
            "sqlite_db": str(self.sqlite_db),
            "runtime_manifest": str(self.runtime_manifest),
        }


@dataclass(frozen=True, slots=True)
class SidecarPorts:
    """Network ports used by the local Temporal sidecar."""

    bind_ip: str
    grpc: int
    ui: int
    source: str
    collisions: tuple[int, ...] = ()

    def temporal_start_args(self) -> tuple[str, ...]:
        """Return Temporal CLI flags for this port policy."""
        return (
            "--ip",
            self.bind_ip,
            "--port",
            str(self.grpc),
            "--ui-port",
            str(self.ui),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible port mapping."""
        return {
            "bind_ip": self.bind_ip,
            "grpc": self.grpc,
            "ui": self.ui,
            "source": self.source,
            "collisions": list(self.collisions),
        }


@dataclass(frozen=True, slots=True)
class SidecarRuntimePolicy:
    """Resolved local runtime policy for one workspace sidecar."""

    workspace: Path
    workspace_id: str
    workspace_slug: str
    runtime_home: Path
    paths: SidecarPaths
    ports: SidecarPorts

    def ensure_directories(self) -> None:
        """Create all runtime directories."""
        self.paths.ensure_directories()

    def temporal_start_args(self) -> tuple[str, ...]:
        """Return Temporal CLI flags for SQLite persistence and ports."""
        return (
            "--db-filename",
            str(self.paths.sqlite_db),
            *self.ports.temporal_start_args(),
        )

    def with_available_ports(
        self,
        port_available: PortAvailabilityProbe,
    ) -> "SidecarRuntimePolicy":
        """Return a policy whose ports have been checked or reallocated."""
        ports = _allocate_ports(
            workspace_id=self.workspace_id,
            bind_ip=self.ports.bind_ip,
            environ={},
            port_available=port_available,
            existing=self.ports,
        )
        return replace(self, ports=ports)

    def write_manifest(self) -> None:
        """Persist the resolved runtime policy for diagnostics."""
        self.ensure_directories()
        self.paths.runtime_manifest.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible runtime policy document."""
        return {
            "workspace": {
                "path": str(self.workspace),
                "id": self.workspace_id,
                "slug": self.workspace_slug,
            },
            "runtime_home": str(self.runtime_home),
            "paths": self.paths.to_dict(),
            "ports": self.ports.to_dict(),
            "data_directory_policy": (
                "Runtime state, logs, manifests, and SQLite files are kept "
                "outside HistData download/cache directories."
            ),
        }


def default_sidecar_runtime_home(
    *,
    environ: Mapping[str, str] | None = None,
    platform_name: str | None = None,
    home: Path | str | None = None,
) -> Path:
    """Return the per-user runtime home for this platform."""
    env = environ if environ is not None else os.environ
    override = env.get(RUNTIME_HOME_ENV)
    if override:
        return Path(override).expanduser()

    user_home = Path(home).expanduser() if home is not None else Path.home()
    system = platform_name or platform.system()
    if system == "Darwin":
        return (
            user_home
            / "Library"
            / "Application Support"
            / "histdatacom"
            / "runtime"
        )
    if system == "Windows":
        local_app_data = env.get("LOCALAPPDATA")
        if local_app_data:
            return Path(local_app_data).expanduser() / "histdatacom" / "runtime"
        return user_home / "AppData" / "Local" / "histdatacom" / "runtime"

    xdg_state_home = env.get("XDG_STATE_HOME")
    if xdg_state_home:
        return Path(xdg_state_home).expanduser() / "histdatacom" / "runtime"
    return user_home / ".local" / "state" / "histdatacom" / "runtime"


def default_sidecar_workspace(
    *,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Return the default workspace used to scope runtime state."""
    env = environ if environ is not None else os.environ
    override = env.get(WORKSPACE_ENV)
    if override:
        return Path(override).expanduser().resolve(strict=False)
    return Path.cwd().resolve(strict=False)


def default_sidecar_state_dir() -> Path:
    """Return the default workspace-scoped runtime state directory."""
    return build_sidecar_runtime_policy().paths.state_dir


def build_sidecar_runtime_policy(
    *,
    workspace: Path | str | None = None,
    runtime_home: Path | str | None = None,
    paths: SidecarPaths | None = None,
    environ: Mapping[str, str] | None = None,
    platform_name: str | None = None,
    home: Path | str | None = None,
    check_ports: bool = False,
    port_available: PortAvailabilityProbe | None = None,
) -> SidecarRuntimePolicy:
    """Build a testable runtime policy for a workspace."""
    env = environ if environ is not None else os.environ
    workspace_path = _resolve_workspace_path(workspace, env)
    workspace_id = _workspace_id(workspace_path, platform_name)
    workspace_slug = _workspace_slug(workspace_path)
    resolved_home = (
        Path(runtime_home).expanduser()
        if runtime_home is not None
        else default_sidecar_runtime_home(
            environ=env,
            platform_name=platform_name,
            home=home,
        )
    )
    runtime_dir = (
        resolved_home / "workspaces" / (f"{workspace_slug}-{workspace_id}")
    )
    resolved_paths = paths or SidecarPaths.from_runtime_dir(runtime_dir)
    if check_ports:
        selected_port_probe = port_available or is_port_available
    else:
        selected_port_probe = _assume_port_available

    ports = _allocate_ports(
        workspace_id=workspace_id,
        bind_ip=env.get(BIND_IP_ENV, DEFAULT_BIND_IP),
        environ=env,
        port_available=selected_port_probe,
    )
    return SidecarRuntimePolicy(
        workspace=workspace_path,
        workspace_id=workspace_id,
        workspace_slug=workspace_slug,
        runtime_home=resolved_home,
        paths=resolved_paths,
        ports=ports,
    )


def is_port_available(bind_ip: str, port: int) -> bool:
    """Return whether a TCP port can be bound on the requested interface."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((bind_ip, port))
        except OSError:
            return False
    return True


def _assume_port_available(bind_ip: str, port: int) -> bool:
    """Skip port probing when only deterministic policy metadata is needed."""
    return True


def _resolve_workspace_path(
    workspace: Path | str | None,
    environ: Mapping[str, str],
) -> Path:
    if workspace is not None:
        return Path(workspace).expanduser().resolve(strict=False)
    return default_sidecar_workspace(environ=environ)


def _workspace_slug(workspace: Path) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", workspace.name.lower())
    return slug.strip(".-_") or "workspace"


def _workspace_id(
    workspace: Path,
    platform_name: str | None,
) -> str:
    normalized = str(workspace)
    if (platform_name or platform.system()) == "Windows":
        normalized = normalized.lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]


def _allocate_ports(
    *,
    workspace_id: str,
    bind_ip: str,
    environ: Mapping[str, str],
    port_available: PortAvailabilityProbe,
    existing: SidecarPorts | None = None,
) -> SidecarPorts:
    if existing and existing.source == "environment":
        return _validate_environment_ports(existing, port_available)

    env_grpc = environ.get(TEMPORAL_PORT_ENV)
    if env_grpc:
        ports = SidecarPorts(
            bind_ip=bind_ip,
            grpc=_parse_port(env_grpc, TEMPORAL_PORT_ENV),
            ui=_parse_port(
                environ.get(TEMPORAL_UI_PORT_ENV, ""),
                TEMPORAL_UI_PORT_ENV,
                default=_parse_port(env_grpc, TEMPORAL_PORT_ENV)
                + DEFAULT_TEMPORAL_UI_PORT_OFFSET,
            ),
            source="environment",
        )
        return _validate_environment_ports(ports, port_available)

    if existing and existing.source == "workspace":
        first_grpc = existing.grpc
        first_offset = first_grpc - DEFAULT_TEMPORAL_PORT_BASE
    else:
        first_offset = int(workspace_id[:8], 16) % DEFAULT_PORT_WINDOW

    collisions: list[int] = []
    for attempt in range(DEFAULT_PORT_SCAN_LIMIT):
        offset = (first_offset + attempt) % DEFAULT_PORT_WINDOW
        grpc = DEFAULT_TEMPORAL_PORT_BASE + offset
        ui = grpc + DEFAULT_TEMPORAL_UI_PORT_OFFSET
        blocked = [
            port for port in (grpc, ui) if not port_available(bind_ip, port)
        ]
        if not blocked:
            return SidecarPorts(
                bind_ip=bind_ip,
                grpc=grpc,
                ui=ui,
                source="workspace",
                collisions=tuple(collisions),
            )
        collisions.extend(blocked)

    raise PortAllocationError(
        "No free Temporal runtime port pair was found for workspace "
        f"{workspace_id} after {DEFAULT_PORT_SCAN_LIMIT} deterministic "
        f"attempts from {DEFAULT_TEMPORAL_PORT_BASE + first_offset}. "
        f"Colliding ports: {tuple(collisions)}."
    )


def _validate_environment_ports(
    ports: SidecarPorts,
    port_available: PortAvailabilityProbe,
) -> SidecarPorts:
    blocked = [
        port
        for port in (ports.grpc, ports.ui)
        if not port_available(ports.bind_ip, port)
    ]
    if blocked:
        raise PortAllocationError(
            "Configured Temporal runtime port is unavailable. "
            f"{TEMPORAL_PORT_ENV}={ports.grpc}, "
            f"{TEMPORAL_UI_PORT_ENV}={ports.ui}, "
            f"blocked={tuple(blocked)}."
        )
    return ports


def _parse_port(
    value: str,
    env_name: str,
    *,
    default: int | None = None,
) -> int:
    if not value and default is not None:
        return default
    try:
        port = int(value)
    except ValueError as err:
        raise PortAllocationError(
            f"{env_name} must be an integer TCP port, got {value!r}."
        ) from err
    if not 0 < port <= 65535:
        raise PortAllocationError(
            f"{env_name} must be between 1 and 65535, got {port}."
        )
    return port

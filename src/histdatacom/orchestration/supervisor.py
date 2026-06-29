"""Local Temporal orchestration process supervision."""

from __future__ import annotations

import json
import os
import signal
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from importlib.util import find_spec
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, cast

from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.orchestration.performance import (
    DEFAULT_INFLUX_WORKERS,
    DEFAULT_NETWORK_MULTIPLIER,
    DEFAULT_ORCHESTRATION_WORKERS,
    OrchestrationConcurrencyProfile,
)
from histdatacom.orchestration.queues import (
    DEFAULT_TASK_QUEUE_PREFIX,
    DEFAULT_TEMPORAL_NAMESPACE,
    OrchestrationTaskQueues,
    OrchestrationWorkerConfig,
    TaskQueueLane,
    build_orchestration_worker_config,
)
from histdatacom.orchestration.readiness import (
    read_worker_readiness,
    remove_worker_readiness,
    worker_readiness_path,
)
from histdatacom.orchestration.resources import (
    current_platform_key,
    default_temporal_runtime_cache_dir,
    inspect_temporal_runtime_cache,
    load_orchestration_manifest,
    load_temporal_runtime_index,
    read_orchestration_asset_text,
    temporal_runtime_executable_path,
)
from histdatacom.orchestration.runtime import (
    PortAvailabilityProbe,
    OrchestrationPaths,
    OrchestrationPorts,
    OrchestrationRuntimePolicy,
    build_orchestration_runtime_policy,
    default_orchestration_state_dir,  # noqa:F401
    is_port_available,
)

ORCHESTRATION_STATE_SCHEMA_VERSION = 1
DEFAULT_STARTUP_TIMEOUT_SECONDS = 10.0
DEFAULT_STOP_TIMEOUT_SECONDS = 10.0
DEFAULT_FRONTEND_PROBE_TIMEOUT_SECONDS = 0.2
DEFAULT_WORKER_LANES = tuple(TaskQueueLane)
WORKER_COMPONENT_PREFIX = "worker:"

ProcessFactory = Callable[..., Any]
CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
ProcessExists = Callable[[int], bool]
ProcessTerminate = Callable[[int], None]
ProcessKill = Callable[[int], None]
WaitPid = Callable[[int, int], tuple[int, int]]
FrontendReadyProbe = Callable[[OrchestrationRuntimePolicy], bool]
WorkerDependencyProbe = Callable[[], bool]


@dataclass(frozen=True, slots=True)
class OrchestrationStatus:
    """Serializable orchestration status for CLI, API, and future GUI callers."""

    state: str
    message: str
    state_dir: str
    pid_file: str
    lock_file: str
    logs: dict[str, str]
    pids: dict[str, int]
    command: tuple[str, ...] = ()
    ports: dict[str, int | str | list[int]] = field(default_factory=dict)
    components: dict[str, str] = field(default_factory=dict)
    worker_readiness: dict[str, dict[str, Any]] = field(default_factory=dict)
    disk: dict[str, Any] = field(default_factory=dict)

    @property
    def running(self) -> bool:
        """Return whether the orchestration is considered healthy enough to reuse."""
        return self.state == "running"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "state": self.state,
            "message": self.message,
            "state_dir": self.state_dir,
            "pid_file": self.pid_file,
            "lock_file": self.lock_file,
            "logs": dict(self.logs),
            "pids": dict(self.pids),
            "command": list(self.command),
            "ports": dict(self.ports),
            "components": dict(self.components),
            "disk": dict(self.disk),
            "worker_readiness": {
                lane: dict(readiness)
                for lane, readiness in self.worker_readiness.items()
            },
        }


def _utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _process_exists(pid: int) -> bool:
    """Return whether a process exists for a PID."""
    if pid <= 0:
        return False
    if _reap_child_process(pid):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _reap_child_process(pid: int) -> bool:
    """Return whether an exited child PID was reaped."""
    waitpid = cast(WaitPid | None, getattr(os, "waitpid", None))
    wnohang = getattr(os, "WNOHANG", None)
    if waitpid is None or not isinstance(wnohang, int) or pid <= 0:
        return False
    try:
        waited_pid, _status = waitpid(pid, wnohang)
    except ChildProcessError:
        return False
    except OSError:
        return False
    return waited_pid == pid


def _terminate_process(pid: int) -> None:
    """Request process termination for a PID."""
    if pid <= 0:
        return
    os.kill(pid, signal.SIGTERM)


def _kill_process(pid: int) -> None:
    """Force process termination for a PID."""
    if pid <= 0:
        return
    os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))


def _namespace_already_exists(
    completed: subprocess.CompletedProcess[str],
) -> bool:
    """Return whether namespace creation failed because it already exists."""
    detail = f"{completed.stdout}\n{completed.stderr}".lower()
    return "already exist" in detail or "already_exist" in detail


def _load_runtime_defaults() -> dict[str, Any]:
    """Load packaged runtime defaults used for command construction."""
    loaded = json.loads(read_orchestration_asset_text("runtime-defaults.json"))
    if not isinstance(loaded, dict):
        raise ValueError("runtime-defaults.json must contain an object")
    return cast(dict[str, Any], loaded)


def _orchestration_state_schema_version(state: Mapping[str, Any]) -> int:
    return int(
        state.get("schema_version", ORCHESTRATION_STATE_SCHEMA_VERSION)
        or ORCHESTRATION_STATE_SCHEMA_VERSION
    )


def _orchestration_state_schema_status(
    path: Path,
    *,
    exists: bool,
    version: int = 0,
    missing_version: bool = False,
    state: str = "",
    error: str = "",
) -> dict[str, Any]:
    if not exists:
        resolved_state = "missing"
    elif state:
        resolved_state = state
    elif missing_version:
        resolved_state = "legacy_unversioned"
        version = 0
    elif version > ORCHESTRATION_STATE_SCHEMA_VERSION:
        resolved_state = "unsupported"
        error = (
            "Unsupported orchestration state schema version "
            f"{version}; expected <= {ORCHESTRATION_STATE_SCHEMA_VERSION}."
        )
    elif version == ORCHESTRATION_STATE_SCHEMA_VERSION:
        resolved_state = "current"
    elif version < 1:
        resolved_state = "invalid"
        error = (
            "Invalid orchestration state schema version "
            f"{version}; expected >= 1."
        )
    else:
        resolved_state = "migration_required"
    return {
        "path": str(path),
        "exists": exists,
        "schema_version": version,
        "expected_schema_version": ORCHESTRATION_STATE_SCHEMA_VERSION,
        "state": resolved_state,
        "error": error,
    }


def build_temporal_start_command(
    executable: Path | str,
    extra_args: Sequence[str] = (),
    *,
    runtime_policy: OrchestrationRuntimePolicy | None = None,
) -> tuple[str, ...]:
    """Build the Temporal server start command."""
    defaults = _load_runtime_defaults()
    runtime_args = (
        runtime_policy.temporal_start_args() if runtime_policy else ()
    )
    args = [
        str(executable),
        *defaults["command"]["args"],
        *runtime_args,
        *extra_args,
    ]
    return tuple(args)


def build_temporal_namespace_describe_command(
    executable: Path | str,
    namespace: str,
    *,
    runtime_policy: OrchestrationRuntimePolicy,
) -> tuple[str, ...]:
    """Build the Temporal CLI command that checks a namespace."""
    return _build_temporal_namespace_command(
        executable,
        "describe",
        namespace,
        runtime_policy=runtime_policy,
    )


def build_temporal_namespace_create_command(
    executable: Path | str,
    namespace: str,
    *,
    runtime_policy: OrchestrationRuntimePolicy,
) -> tuple[str, ...]:
    """Build the Temporal CLI command that creates a namespace."""
    return _build_temporal_namespace_command(
        executable,
        "create",
        namespace,
        runtime_policy=runtime_policy,
    )


def _build_temporal_namespace_command(
    executable: Path | str,
    action: str,
    namespace: str,
    *,
    runtime_policy: OrchestrationRuntimePolicy,
) -> tuple[str, ...]:
    target_host = f"{runtime_policy.ports.bind_ip}:{runtime_policy.ports.grpc}"
    return (
        str(executable),
        "operator",
        "namespace",
        action,
        "--address",
        target_host,
        "--namespace",
        namespace,
        "--command-timeout",
        "10s",
    )


def build_orchestration_worker_start_command(
    config: OrchestrationWorkerConfig,
) -> tuple[str, ...]:
    """Build the worker lane subprocess command."""
    profile = config.concurrency_profile
    return (
        sys.executable,
        "-m",
        "histdatacom.orchestration.worker",
        "--workspace",
        str(config.runtime_policy.workspace),
        "--runtime-home",
        str(config.runtime_policy.runtime_home),
        "--state-dir",
        str(config.runtime_policy.paths.state_dir),
        "run",
        "--namespace",
        config.namespace,
        "--task-queue-prefix",
        config.task_queues.prefix,
        "--lane",
        config.lane.value,
        "--cpu-utilization",
        profile.cpu_utilization,
        "--network-multiplier",
        str(profile.network_multiplier),
        "--orchestration-workers",
        str(profile.orchestration_workers),
        "--influx-workers",
        str(profile.influx_workers),
        "--max-concurrent-activities",
        str(profile.workers_for_lane(config.lane)),
    )


def _temporal_worker_dependency_available() -> bool:
    """Return whether the Temporal SDK is importable for worker processes."""
    return find_spec("temporalio") is not None


def _temporal_frontend_ready(
    runtime_policy: OrchestrationRuntimePolicy,
) -> bool:
    """Return whether the Temporal frontend socket accepts connections."""
    ports = runtime_policy.ports
    try:
        with socket.create_connection(
            (ports.bind_ip, ports.grpc),
            timeout=DEFAULT_FRONTEND_PROBE_TIMEOUT_SECONDS,
        ):
            return True
    except OSError:
        return False


class OrchestrationSupervisor:
    """Supervise the local Temporal orchestration process group."""

    def __init__(
        self,
        paths: OrchestrationPaths | None = None,
        *,
        runtime_policy: OrchestrationRuntimePolicy | None = None,
        process_exists: ProcessExists = _process_exists,
        process_terminate: ProcessTerminate = _terminate_process,
        process_kill: ProcessKill = _kill_process,
        process_factory: ProcessFactory = subprocess.Popen,
        command_runner: CommandRunner = subprocess.run,
        port_available: PortAvailabilityProbe = is_port_available,
        frontend_ready: FrontendReadyProbe = _temporal_frontend_ready,
        worker_dependency_available: WorkerDependencyProbe = (
            _temporal_worker_dependency_available
        ),
        sleep: Callable[[float], None] = time.sleep,
        namespace: str = DEFAULT_TEMPORAL_NAMESPACE,
        task_queue_prefix: str = DEFAULT_TASK_QUEUE_PREFIX,
        worker_lanes: Sequence[str | TaskQueueLane] = DEFAULT_WORKER_LANES,
        cpu_utilization: str | int | None = "medium",
        network_multiplier: int = DEFAULT_NETWORK_MULTIPLIER,
        orchestration_workers: int = DEFAULT_ORCHESTRATION_WORKERS,
        influx_workers: int = DEFAULT_INFLUX_WORKERS,
    ) -> None:
        """Initialize the orchestration supervisor."""
        self.runtime_policy: OrchestrationRuntimePolicy = (
            runtime_policy or build_orchestration_runtime_policy(paths=paths)
        )
        self.paths: OrchestrationPaths = self.runtime_policy.paths
        self._process_exists = process_exists
        self._process_terminate = process_terminate
        self._process_kill = process_kill
        self._process_factory = process_factory
        self._command_runner = command_runner
        self._port_available = port_available
        self._frontend_ready = frontend_ready
        self._worker_dependency_available = worker_dependency_available
        self._sleep = sleep
        self.namespace = namespace.strip() or DEFAULT_TEMPORAL_NAMESPACE
        self.task_queue_prefix = task_queue_prefix
        self.worker_lanes = tuple(
            TaskQueueLane.from_value(lane) for lane in worker_lanes
        )
        self.cpu_utilization = cpu_utilization
        self.network_multiplier = network_multiplier
        self.orchestration_workers = orchestration_workers
        self.influx_workers = influx_workers

    def status(self, *, repair: bool = False) -> OrchestrationStatus:
        """Return current orchestration process status."""
        if not self.paths.pid_file.exists():
            return self._status(
                "stopped", "Orchestration is not running.", {}, ()
            )

        try:
            state = self._read_state()
        except (OSError, ValueError, json.JSONDecodeError) as err:
            if repair:
                self._remove_state_files()
            return self._status(
                "stale",
                f"Orchestration state is unreadable: {err}",
                {},
                (),
            )

        pids = self._state_pids(state)
        command = tuple(str(item) for item in state.get("command", []))
        ports = self._state_ports(state)
        logs = self._state_logs(state)
        if not pids:
            if repair:
                self._remove_state_files()
            return self._status(
                "stale",
                "Orchestration state does not contain any valid process IDs.",
                {},
                command,
                ports,
                logs=logs,
            )

        worker_readiness = self._worker_readiness_states(pids)
        component_states = self._component_states(pids, worker_readiness)
        missing_required = tuple(
            component
            for component in self._required_components()
            if component not in pids
        )
        missing = {
            component: pid
            for component, pid in pids.items()
            if not self._process_exists(pid)
        }
        not_ready_required = tuple(
            self._worker_component(lane)
            for lane in self.worker_lanes
            if self._worker_component(lane) in pids
            and self._worker_component(lane) not in missing
            and worker_readiness.get(lane.value, {}).get("state") != "ready"
        )
        if not missing and not missing_required and not not_ready_required:
            return self._status(
                "running",
                "Orchestration server and worker lanes are running.",
                pids,
                command,
                ports,
                logs=logs,
                components=component_states,
                worker_readiness=worker_readiness,
            )

        if repair:
            self._terminate_pids(pids)
            self._remove_state_files()
        details = []
        if missing_required:
            details.append(f"missing components: {list(missing_required)}")
        if missing:
            details.append(f"dead processes: {missing}")
        if not_ready_required:
            details.append(f"workers not ready: {list(not_ready_required)}")
        return self._status(
            "stale",
            f"Orchestration state is incomplete: {'; '.join(details)}.",
            pids,
            command,
            ports,
            logs=logs,
            components=component_states,
            worker_readiness=worker_readiness,
        )

    def client_worker_config(
        self,
        *,
        status: OrchestrationStatus | None = None,
        require_running: bool = False,
    ) -> OrchestrationWorkerConfig:
        """Resolve client config from running orchestration state when available."""
        current = status or self.status(repair=False)
        if current.running:
            return self._client_worker_config_from_running_state(current)
        if require_running:
            raise RuntimeError(
                "Cannot resolve Temporal client configuration because the "
                f"orchestration is {current.state}: {current.message}"
            )
        return self._worker_config(self.runtime_policy)

    def start(
        self,
        *,
        executable: Path | str | None = None,
        extra_args: Sequence[str] = (),
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT_SECONDS,
    ) -> OrchestrationStatus:
        """Start the orchestration, or return running status if already healthy."""
        current = self.status(repair=True)
        if current.running:
            return self._status(
                "running",
                "Orchestration is already running.",
                current.pids,
                current.command,
                current.ports,
                logs=current.logs,
                components=current.components,
                worker_readiness=current.worker_readiness,
            )

        self.paths.state_dir.mkdir(parents=True, exist_ok=True)
        runtime_policy = self.runtime_policy.with_available_ports(
            self._port_available
        )
        self.runtime_policy = runtime_policy
        self._acquire_lock()
        try:
            if executable is None:
                with temporal_runtime_executable_path() as resolution:
                    return self._start_process(
                        resolution.executable,
                        extra_args,
                        startup_timeout,
                        runtime_policy,
                        runtime_resolution=resolution.to_dict(),
                    )
            return self._start_process(
                Path(executable).expanduser(),
                extra_args,
                startup_timeout,
                runtime_policy,
                runtime_resolution={"source": "explicit"},
            )
        finally:
            self._release_lock()

    def stop(
        self,
        *,
        stop_timeout: float = DEFAULT_STOP_TIMEOUT_SECONDS,
    ) -> OrchestrationStatus:
        """Stop all known orchestration processes and remove persisted state."""
        self._acquire_lock()
        try:
            current = self.status(repair=False)
            if current.state == "stopped":
                return current
            if current.state == "stale":
                self._terminate_and_wait(current.pids, stop_timeout)
                self._remove_state_files()
                return self._status(
                    "stopped",
                    "Removed stale orchestration state and terminated known processes.",
                    {},
                    (),
                )

            still_running = self._terminate_and_wait(
                current.pids,
                stop_timeout,
            )
            if still_running:
                return self._status(
                    "stopping",
                    f"Orchestration processes still running: {still_running}.",
                    still_running,
                    current.command,
                )
            self._remove_state_files()
            return self._status(
                "stopped",
                "Orchestration stopped.",
                {},
                (),
            )
        finally:
            self._release_lock()

    def restart(
        self,
        *,
        executable: Path | str | None = None,
        extra_args: Sequence[str] = (),
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT_SECONDS,
        stop_timeout: float = DEFAULT_STOP_TIMEOUT_SECONDS,
    ) -> OrchestrationStatus:
        """Restart the orchestration."""
        self.stop(stop_timeout=stop_timeout)
        return self.start(
            executable=executable,
            extra_args=extra_args,
            startup_timeout=startup_timeout,
        )

    def doctor(self) -> dict[str, Any]:
        """Return supervisor diagnostics without changing orchestration state."""
        status = self.status(repair=False)
        runtime_policy = self._runtime_policy_from_status(status)
        manifest = load_orchestration_manifest()
        runtime_index = load_temporal_runtime_index(manifest)
        platform_key = current_platform_key()
        platform_resource = manifest.platforms.get(platform_key)
        platform_artifact = runtime_index.platforms.get(platform_key)
        executable_bundled = (
            bool(platform_resource.bundled) if platform_resource else False
        )
        cache_entries = inspect_temporal_runtime_cache()
        current_cache_entries = tuple(
            entry
            for entry in cache_entries
            if platform_artifact is not None
            and entry.version == runtime_index.version
            and entry.platform_key == platform_key
            and entry.archive_sha256 == platform_artifact.archive_sha256
        )
        cache_available = any(entry.valid for entry in current_cache_entries)
        worker_status = self._worker_status(
            status.pids,
            status.components,
            status.worker_readiness,
        )
        return {
            "status": status.to_dict(),
            "paths": self._path_dict(),
            "components": dict(status.components),
            "workers": worker_status,
            "frontend": {
                "target_host": (
                    f"{runtime_policy.ports.bind_ip}:"
                    f"{runtime_policy.ports.grpc}"
                ),
                "ready": (
                    self._frontend_ready(runtime_policy)
                    if status.running
                    else False
                ),
            },
            "platform": {
                "key": platform_key,
                "supported": platform_resource is not None,
                "executable_bundled": executable_bundled,
                "message": (
                    "No packaged Temporal executable is available in this "
                    "artifact. The runtime resolver will use a verified cache "
                    "entry, provision the pinned executable on first use, use "
                    "an offline/private bundled wheel, or honor --executable."
                    if not executable_bundled
                    else "Packaged Temporal executable is available."
                ),
            },
            "runtime_provisioning": {
                "index_version": runtime_index.version,
                "cache_dir": str(default_temporal_runtime_cache_dir()),
                "cache_available": cache_available,
                "cache_entries": [
                    entry.to_dict() for entry in current_cache_entries
                ],
                "artifact": (
                    {
                        "archive_name": platform_artifact.archive_name,
                        "archive_url": platform_artifact.archive_url,
                        "archive_sha256": platform_artifact.archive_sha256,
                        "archive_size_bytes": platform_artifact.archive_size_bytes,
                        "executable_name": platform_artifact.executable_name,
                    }
                    if platform_artifact is not None
                    else None
                ),
            },
            "runtime_defaults": _load_runtime_defaults(),
            "runtime_policy": runtime_policy.to_dict(),
            "persistence": {
                "status_store": ManifestStatusStore.inspect_schema(
                    self.paths.manifests_dir
                ),
                "orchestration_state": self._state_schema_diagnostics(),
            },
        }

    def _start_process(
        self,
        executable: Path,
        extra_args: Sequence[str],
        startup_timeout: float,
        runtime_policy: OrchestrationRuntimePolicy,
        *,
        runtime_resolution: Mapping[str, Any] | None = None,
    ) -> OrchestrationStatus:
        """Start the Temporal server and worker lane fleet."""
        if self.worker_lanes and not self._worker_dependency_available():
            raise RuntimeError(
                "Temporal worker support requires temporalio. Base "
                "histdatacom installs include this dependency; reinstall "
                "histdatacom with dependencies enabled or install the "
                "temporal compatibility extra before starting the orchestration "
                "worker fleet."
            )

        server_command = build_temporal_start_command(
            executable,
            extra_args,
            runtime_policy=runtime_policy,
        )
        runtime_policy.write_manifest()
        pids: dict[str, int] = {}
        commands: dict[str, list[str]] = {}
        logs: dict[str, str] = {"server": str(self.paths.server_log)}
        worker_readiness: dict[str, dict[str, Any]] = {}
        deadline = time.monotonic() + startup_timeout
        try:
            remove_worker_readiness(self.paths.state_dir)
            server_process = self._launch_component(
                server_command,
                self.paths.server_log,
            )
            pids["server"] = int(server_process.pid)
            commands["server"] = list(server_command)
            if not self._process_running(server_process):
                raise RuntimeError(
                    "Temporal server exited during startup. "
                    f"See log: {self.paths.server_log}"
                )
            self._wait_for_frontend(server_process, runtime_policy, deadline)
            self._ensure_namespace(executable, runtime_policy)

            base_worker_config = self._worker_config(runtime_policy)
            for lane in self.worker_lanes:
                worker_config = base_worker_config.for_lane(lane)
                worker_command = build_orchestration_worker_start_command(
                    worker_config
                )
                component = self._worker_component(lane)
                log_path = self._worker_log_path(lane)
                remove_worker_readiness(self.paths.state_dir, lane)
                worker_process = self._launch_component(
                    worker_command,
                    log_path,
                )
                if not self._process_running(worker_process):
                    raise RuntimeError(
                        f"Temporal worker lane {lane.value!r} exited during "
                        f"startup. See log: {log_path}"
                    )
                pids[component] = int(worker_process.pid)
                commands[component] = list(worker_command)
                logs[component] = str(log_path)
                worker_readiness[lane.value] = self._wait_for_worker_ready(
                    lane,
                    int(worker_process.pid),
                    worker_process,
                    deadline,
                    log_path,
                )

            state = {
                "schema_version": ORCHESTRATION_STATE_SCHEMA_VERSION,
                "started_at_utc": _utc_now(),
                "command": list(server_command),
                "commands": commands,
                "pids": pids,
                "ports": runtime_policy.ports.to_dict(),
                "runtime_policy": runtime_policy.to_dict(),
                "runtime_resolution": dict(runtime_resolution or {}),
                "worker_fleet": self._worker_fleet_metadata(base_worker_config),
                "worker_readiness": worker_readiness,
                "logs": logs,
            }
            self._write_state(state)
            components = self._component_states(pids, worker_readiness)
            return self._status(
                "running",
                "Orchestration server and worker lanes started.",
                pids,
                server_command,
                logs=logs,
                components=components,
                worker_readiness=worker_readiness,
            )
        except Exception:
            self._terminate_pids(pids)
            raise

    def _acquire_lock(self) -> None:
        """Create the transient supervisor lock file."""
        self.paths.state_dir.mkdir(parents=True, exist_ok=True)
        if self.paths.lock_file.exists():
            try:
                lock_data = json.loads(
                    self.paths.lock_file.read_text(encoding="utf-8")
                )
                owner_pid = int(lock_data.get("owner_pid", 0))
            except (OSError, ValueError, json.JSONDecodeError):
                owner_pid = 0
            if owner_pid and self._process_exists(owner_pid):
                raise RuntimeError(
                    f"Orchestration lock is held by live process {owner_pid}."
                )
            self.paths.lock_file.unlink(missing_ok=True)

        self.paths.lock_file.write_text(
            json.dumps(
                {"owner_pid": os.getpid(), "created_at_utc": _utc_now()}
            ),
            encoding="utf-8",
        )

    def _release_lock(self) -> None:
        """Remove the transient supervisor lock file."""
        self.paths.lock_file.unlink(missing_ok=True)

    def _read_state(self) -> dict[str, Any]:
        """Read persisted orchestration process state."""
        loaded = json.loads(self.paths.pid_file.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("orchestration state must contain an object")
        return self._normalize_state_schema(cast(dict[str, Any], loaded))

    def _write_state(self, state: Mapping[str, Any]) -> None:
        """Write persisted orchestration process state."""
        self.paths.pid_file.write_text(
            json.dumps(dict(state), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _normalize_state_schema(
        self,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        """Return persisted state normalized to the current schema version."""
        try:
            version = _orchestration_state_schema_version(state)
        except (TypeError, ValueError) as err:
            raise ValueError(
                f"Invalid orchestration state schema version: {err}"
            ) from err
        if version > ORCHESTRATION_STATE_SCHEMA_VERSION:
            raise ValueError(
                "Unsupported orchestration state schema version "
                f"{version}; expected <= {ORCHESTRATION_STATE_SCHEMA_VERSION}. "
                "Upgrade histdatacom before reusing this state."
            )
        if version < 1:
            raise ValueError(
                "Invalid orchestration state schema version "
                f"{version}; expected >= 1."
            )
        normalized = dict(state)
        normalized["schema_version"] = ORCHESTRATION_STATE_SCHEMA_VERSION
        return normalized

    def _state_schema_diagnostics(self) -> dict[str, Any]:
        """Return orchestration state JSON schema diagnostics without mutation."""
        if not self.paths.pid_file.exists():
            return _orchestration_state_schema_status(
                self.paths.pid_file,
                exists=False,
            )
        try:
            loaded = json.loads(self.paths.pid_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as err:
            return _orchestration_state_schema_status(
                self.paths.pid_file,
                exists=True,
                state="error",
                error=str(err),
            )
        if not isinstance(loaded, Mapping):
            return _orchestration_state_schema_status(
                self.paths.pid_file,
                exists=True,
                state="error",
                error="orchestration state must contain an object",
            )
        try:
            version = _orchestration_state_schema_version(loaded)
        except (TypeError, ValueError) as err:
            return _orchestration_state_schema_status(
                self.paths.pid_file,
                exists=True,
                state="error",
                error=f"invalid schema_version: {err}",
            )
        return _orchestration_state_schema_status(
            self.paths.pid_file,
            exists=True,
            version=version,
            missing_version="schema_version" not in loaded,
        )

    def _state_pids(self, state: Mapping[str, Any]) -> dict[str, int]:
        """Return valid component PID values from persisted state."""
        raw_pids = state.get("pids") or {}
        if not isinstance(raw_pids, Mapping):
            return {}

        pids: dict[str, int] = {}
        for component, pid in raw_pids.items():
            try:
                parsed_pid = int(pid)
            except (TypeError, ValueError):
                continue
            if parsed_pid > 0:
                pids[str(component)] = parsed_pid
        return pids

    def _state_logs(self, state: Mapping[str, Any]) -> dict[str, str]:
        """Return persisted component log paths when available."""
        raw_logs = state.get("logs") or {}
        if not isinstance(raw_logs, Mapping):
            return self._default_logs()
        logs = {
            str(component): str(path)
            for component, path in raw_logs.items()
            if isinstance(component, str) and isinstance(path, str)
        }
        return logs or self._default_logs()

    def _state_ports(
        self,
        state: Mapping[str, Any],
    ) -> dict[str, int | str | list[int]]:
        """Return persisted runtime port values when available."""
        ports = state.get("ports")
        if isinstance(ports, Mapping):
            return {
                str(key): value
                for key, value in ports.items()
                if isinstance(value, (int, str, list))
            }
        return cast(
            dict[str, int | str | list[int]],
            self.runtime_policy.ports.to_dict(),
        )

    def _worker_config(
        self,
        runtime_policy: OrchestrationRuntimePolicy,
    ) -> OrchestrationWorkerConfig:
        """Return base worker configuration for the supervised fleet."""
        return build_orchestration_worker_config(
            runtime_policy=runtime_policy,
            namespace=self.namespace,
            task_queue_prefix=self.task_queue_prefix,
            cpu_utilization=self.cpu_utilization,
            network_multiplier=self.network_multiplier,
            orchestration_workers=self.orchestration_workers,
            influx_workers=self.influx_workers,
        )

    def _client_worker_config_from_running_state(
        self,
        status: OrchestrationStatus,
    ) -> OrchestrationWorkerConfig:
        """Build the client config represented by the persisted live state."""
        state = self._read_running_state()
        runtime_policy = self._runtime_policy_from_running_state(
            state,
            status,
        )
        worker_fleet = self._state_worker_fleet(state)
        namespace = str(worker_fleet.get("namespace", "") or "").strip()
        if not namespace:
            raise RuntimeError(
                "Running orchestration state is missing worker_fleet.namespace; "
                "restart the orchestration before submitting jobs."
            )
        return OrchestrationWorkerConfig(
            runtime_policy=runtime_policy,
            namespace=namespace,
            task_queues=self._state_task_queues(
                worker_fleet,
                runtime_policy,
            ),
            concurrency=self._state_concurrency_profile(worker_fleet),
        )

    def _read_running_state(self) -> dict[str, Any]:
        """Read the running orchestration state or raise a client-facing error."""
        try:
            return self._read_state()
        except (OSError, ValueError, json.JSONDecodeError) as err:
            raise RuntimeError(
                "Running orchestration state could not be read; stop and restart "
                "the orchestration before submitting jobs."
            ) from err

    def _runtime_policy_from_status(
        self,
        status: OrchestrationStatus,
    ) -> OrchestrationRuntimePolicy:
        """Return a policy using the persisted status ports when valid."""
        try:
            return replace(
                self.runtime_policy,
                ports=self._state_ports_from_mapping(status.ports),
            )
        except RuntimeError:
            return self.runtime_policy

    def _runtime_policy_from_running_state(
        self,
        state: Mapping[str, Any],
        status: OrchestrationStatus,
    ) -> OrchestrationRuntimePolicy:
        """Return runtime policy represented by persisted running state."""
        runtime_policy = state.get("runtime_policy")
        ports_payload: object = None
        if isinstance(runtime_policy, Mapping):
            ports_payload = runtime_policy.get("ports")
        if not isinstance(ports_payload, Mapping):
            ports_payload = state.get("ports")
        if not isinstance(ports_payload, Mapping):
            ports_payload = status.ports
        ports = self._state_ports_from_mapping(ports_payload)
        return replace(self.runtime_policy, ports=ports)

    def _state_ports_from_mapping(
        self,
        payload: Mapping[str, Any],
    ) -> OrchestrationPorts:
        """Parse persisted orchestration ports from state/status metadata."""
        bind_ip = str(
            payload.get("bind_ip") or self.runtime_policy.ports.bind_ip
        )
        grpc = self._state_int(payload, "grpc", minimum=1)
        ui = self._state_int(payload, "ui", minimum=1)
        collisions_payload = payload.get("collisions", ())
        collisions: tuple[int, ...] = ()
        if isinstance(collisions_payload, list | tuple):
            collisions = tuple(
                int(value)
                for value in collisions_payload
                if isinstance(value, int | str)
            )
        return OrchestrationPorts(
            bind_ip=bind_ip,
            grpc=grpc,
            ui=ui,
            source=str(payload.get("source") or "state"),
            collisions=collisions,
        )

    def _state_worker_fleet(
        self,
        state: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Return persisted worker fleet metadata or fail clearly."""
        worker_fleet = state.get("worker_fleet")
        if not isinstance(worker_fleet, Mapping):
            raise RuntimeError(
                "Running orchestration state is missing worker_fleet metadata; "
                "restart the orchestration before submitting jobs."
            )
        return worker_fleet

    def _state_task_queues(
        self,
        worker_fleet: Mapping[str, Any],
        runtime_policy: OrchestrationRuntimePolicy,
    ) -> OrchestrationTaskQueues:
        """Return exact task queues persisted for the running worker fleet."""
        task_queues = worker_fleet.get("task_queues")
        if not isinstance(task_queues, Mapping):
            raise RuntimeError(
                "Running orchestration state is missing worker_fleet.task_queues; "
                "restart the orchestration before submitting jobs."
            )
        prefix = str(
            task_queues.get("prefix")
            or worker_fleet.get("task_queue_prefix")
            or ""
        ).strip()
        values = {
            key: str(task_queues.get(key, "") or "").strip()
            for key in ("orchestration", "network", "cpu_file", "influx")
        }
        missing = [key for key, value in values.items() if not value]
        if not prefix or missing:
            raise RuntimeError(
                "Running orchestration state has malformed worker task queues; "
                f"missing={missing or ['prefix']}. Restart the orchestration before "
                "submitting jobs."
            )
        return OrchestrationTaskQueues(
            prefix=prefix,
            workspace_id=str(
                task_queues.get("workspace_id") or runtime_policy.workspace_id
            ),
            orchestration=values["orchestration"],
            network=values["network"],
            cpu_file=values["cpu_file"],
            influx=values["influx"],
        )

    def _state_concurrency_profile(
        self,
        worker_fleet: Mapping[str, Any],
    ) -> OrchestrationConcurrencyProfile | None:
        """Return persisted concurrency policy when present and valid."""
        concurrency = worker_fleet.get("concurrency")
        if not isinstance(concurrency, Mapping):
            return None
        try:
            return OrchestrationConcurrencyProfile(
                cpu_utilization=str(
                    concurrency.get("cpu_utilization")
                    or self.cpu_utilization
                    or "medium"
                ),
                base_workers=self._state_int(
                    concurrency,
                    "base_workers",
                    minimum=1,
                ),
                orchestration_workers=self._state_int(
                    concurrency,
                    "orchestration_workers",
                    minimum=1,
                ),
                network_workers=self._state_int(
                    concurrency,
                    "network_workers",
                    minimum=1,
                ),
                cpu_file_workers=self._state_int(
                    concurrency,
                    "cpu_file_workers",
                    minimum=1,
                ),
                influx_workers=self._state_int(
                    concurrency,
                    "influx_workers",
                    minimum=1,
                ),
                network_multiplier=self._state_int(
                    concurrency,
                    "network_multiplier",
                    minimum=1,
                ),
                source=str(concurrency.get("source") or "state"),
            )
        except RuntimeError as err:
            raise RuntimeError(
                "Running orchestration state has malformed worker concurrency "
                "metadata; restart the orchestration before submitting jobs."
            ) from err

    def _state_int(
        self,
        payload: Mapping[str, Any],
        key: str,
        *,
        minimum: int,
    ) -> int:
        """Parse an integer field from persisted orchestration state."""
        try:
            value = int(payload[key])
        except (KeyError, TypeError, ValueError) as err:
            raise RuntimeError(
                f"Running orchestration state is missing integer field {key!r}."
            ) from err
        if value < minimum:
            raise RuntimeError(
                f"Running orchestration state field {key!r} must be >= {minimum}."
            )
        return value

    def _launch_component(
        self,
        command: Sequence[str],
        log_path: Path,
    ) -> Any:
        """Launch one orchestration component process."""
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log = log_path.open("ab")
        try:
            return self._process_factory(
                list(command),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                close_fds=os.name != "nt",
                start_new_session=os.name != "nt",
            )
        finally:
            log.close()

    def _process_running(self, process: Any) -> bool:
        """Return whether a just-started process is still running."""
        poll = getattr(process, "poll", lambda: None)
        return poll() is None

    def _ensure_namespace(
        self,
        executable: Path,
        runtime_policy: OrchestrationRuntimePolicy,
    ) -> None:
        """Ensure a non-default local namespace exists before workers start."""
        if self.namespace == DEFAULT_TEMPORAL_NAMESPACE:
            return

        describe = self._run_temporal_command(
            build_temporal_namespace_describe_command(
                executable,
                self.namespace,
                runtime_policy=runtime_policy,
            )
        )
        if describe.returncode == 0:
            return

        create = self._run_temporal_command(
            build_temporal_namespace_create_command(
                executable,
                self.namespace,
                runtime_policy=runtime_policy,
            )
        )
        if create.returncode == 0 or _namespace_already_exists(create):
            return

        raise RuntimeError(
            "Temporal namespace could not be created: "
            f"{self.namespace!r}; stdout={create.stdout!r}; "
            f"stderr={create.stderr!r}"
        )

    def _run_temporal_command(
        self,
        command: Sequence[str],
    ) -> subprocess.CompletedProcess[str]:
        """Run a short Temporal CLI control-plane command."""
        return self._command_runner(
            list(command),
            capture_output=True,
            check=False,
            text=True,
        )

    def _wait_for_frontend(
        self,
        server_process: Any,
        runtime_policy: OrchestrationRuntimePolicy,
        deadline: float,
    ) -> None:
        """Wait until the Temporal frontend accepts local connections."""
        while time.monotonic() < deadline:
            if not self._process_running(server_process):
                raise RuntimeError(
                    "Temporal server exited before the frontend became ready."
                )
            if self._frontend_ready(runtime_policy):
                return
            self._sleep(0.05)
        raise RuntimeError(
            "Temporal frontend did not become ready before startup timeout."
        )

    def _wait_for_worker_ready(
        self,
        lane: TaskQueueLane,
        pid: int,
        worker_process: Any,
        deadline: float,
        log_path: Path,
    ) -> dict[str, Any]:
        """Wait until a worker lane publishes a valid readiness marker."""
        while time.monotonic() < deadline:
            if not self._process_running(worker_process):
                raise RuntimeError(
                    f"Temporal worker lane {lane.value!r} exited before "
                    f"readiness. See log: {log_path}"
                )
            readiness = self._worker_readiness_state(lane, pid)
            if readiness["state"] == "ready":
                return readiness
            self._sleep(0.05)
        if not self._process_running(worker_process):
            raise RuntimeError(
                f"Temporal worker lane {lane.value!r} exited before "
                f"readiness. See log: {log_path}"
            )
        raise RuntimeError(
            f"Temporal worker lane {lane.value!r} did not report readiness "
            f"before startup timeout. See log: {log_path}"
        )

    def _required_components(self) -> tuple[str, ...]:
        """Return component IDs required for a healthy orchestration."""
        return (
            "server",
            *(self._worker_component(lane) for lane in self.worker_lanes),
        )

    def _component_states(
        self,
        pids: Mapping[str, int],
        worker_readiness: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, str]:
        """Return component health by required and known component ID."""
        states: dict[str, str] = {}
        for component in self._required_components():
            pid = pids.get(component)
            if pid is None or not self._process_exists(pid):
                states[component] = "missing"
                continue
            lane = self._component_lane(component)
            if lane is None or worker_readiness is None:
                states[component] = "running"
                continue
            states[component] = (
                "running"
                if worker_readiness.get(lane.value, {}).get("state") == "ready"
                else str(
                    worker_readiness.get(lane.value, {}).get(
                        "state",
                        "not_ready",
                    )
                )
            )
        for component, pid in pids.items():
            if component in states:
                continue
            states[component] = (
                "running" if self._process_exists(pid) else "dead"
            )
        return states

    def _worker_status(
        self,
        pids: Mapping[str, int],
        component_states: Mapping[str, str],
        worker_readiness: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Return worker lane diagnostics for doctor output."""
        workers: dict[str, dict[str, Any]] = {}
        for lane in self.worker_lanes:
            component = self._worker_component(lane)
            readiness = dict(
                worker_readiness.get(lane.value)
                or self._worker_readiness_state(
                    lane,
                    int(pids.get(component, 0)),
                )
            )
            workers[lane.value] = {
                "component": component,
                "pid": int(pids.get(component, 0)),
                "state": str(component_states.get(component, "missing")),
                "log": str(self._worker_log_path(lane)),
                "ready": bool(readiness.get("ready")),
                "readiness_state": str(readiness.get("state", "missing")),
                "readiness": readiness,
            }
        return workers

    def _worker_readiness_states(
        self,
        pids: Mapping[str, int],
    ) -> dict[str, dict[str, Any]]:
        """Return readiness diagnostics for all configured worker lanes."""
        return {
            lane.value: self._worker_readiness_state(
                lane,
                int(pids.get(self._worker_component(lane), 0)),
            )
            for lane in self.worker_lanes
        }

    def _worker_readiness_state(
        self,
        lane: TaskQueueLane,
        pid: int,
    ) -> dict[str, Any]:
        """Return a validated worker readiness state for one lane."""
        component = self._worker_component(lane)
        marker_path = worker_readiness_path(self.paths.state_dir, lane)
        base: dict[str, Any] = {
            "component": component,
            "lane": lane.value,
            "pid": int(pid),
            "path": str(marker_path),
            "ready": False,
        }
        if pid <= 0:
            return {
                **base,
                "state": "missing",
                "message": "Worker PID is missing.",
            }
        if not self._process_exists(pid):
            return {
                **base,
                "state": "dead",
                "message": f"Worker PID {pid} is not running.",
            }
        payload = read_worker_readiness(self.paths.state_dir, lane)
        if payload is None:
            return {
                **base,
                "state": "not_ready",
                "message": "Worker readiness marker has not been written.",
            }
        payload_pid = self._readiness_pid(payload)
        payload_lane = str(payload.get("lane", ""))
        if payload_pid != pid or payload_lane != lane.value:
            return {
                **base,
                "state": "stale",
                "message": (
                    "Worker readiness marker does not match the live "
                    "worker lane and PID."
                ),
                "marker": payload,
            }
        if str(payload.get("state", "")) != "ready":
            return {
                **base,
                "state": "not_ready",
                "message": str(payload.get("message", "Worker is not ready.")),
                "marker": payload,
            }
        return {
            **base,
            **payload,
            "pid": pid,
            "state": "ready",
            "ready": True,
            "path": str(marker_path),
        }

    def _readiness_pid(self, payload: Mapping[str, Any]) -> int:
        """Return a parsed readiness PID, or zero when malformed."""
        try:
            return int(payload.get("pid", 0))
        except (TypeError, ValueError):
            return 0

    def _worker_fleet_metadata(
        self,
        config: OrchestrationWorkerConfig,
    ) -> dict[str, Any]:
        """Return persisted worker fleet configuration metadata."""
        return {
            "namespace": config.namespace,
            "task_queue_prefix": config.task_queues.prefix,
            "task_queues": config.task_queues.to_dict(),
            "lanes": [lane.value for lane in self.worker_lanes],
            "concurrency": config.concurrency_profile.to_dict(),
        }

    def _worker_component(self, lane: TaskQueueLane) -> str:
        """Return the persisted component name for a worker lane."""
        return f"{WORKER_COMPONENT_PREFIX}{lane.value}"

    def _component_lane(self, component: str) -> TaskQueueLane | None:
        """Return the worker lane for a component ID when applicable."""
        if not component.startswith(WORKER_COMPONENT_PREFIX):
            return None
        try:
            return TaskQueueLane.from_value(
                component.removeprefix(WORKER_COMPONENT_PREFIX)
            )
        except ValueError:
            return None

    def _worker_log_path(self, lane: TaskQueueLane) -> Path:
        """Return the lane-specific worker log path."""
        return Path(self.paths.logs_dir) / f"temporal-worker-{lane.value}.log"

    def _default_logs(self) -> dict[str, str]:
        """Return default component log paths."""
        return {
            "server": str(self.paths.server_log),
            **{
                self._worker_component(lane): str(self._worker_log_path(lane))
                for lane in self.worker_lanes
            },
        }

    def _terminate_and_wait(
        self,
        pids: Mapping[str, int],
        stop_timeout: float,
    ) -> dict[str, int]:
        """Terminate known component PIDs and return any still running."""
        self._terminate_pids(pids)
        still_running = self._wait_for_process_exit(pids, stop_timeout)
        if not still_running:
            return {}

        self._kill_pids(still_running)
        kill_timeout = min(5.0, max(0.0, stop_timeout))
        return self._wait_for_process_exit(still_running, kill_timeout)

    def _wait_for_process_exit(
        self,
        pids: Mapping[str, int],
        timeout: float,
    ) -> dict[str, int]:
        """Return PIDs that still exist after waiting up to timeout seconds."""
        deadline = time.monotonic() + max(0.0, timeout)
        while time.monotonic() < deadline:
            if not any(self._process_exists(pid) for pid in pids.values()):
                break
            self._sleep(0.05)
        return {
            component: pid
            for component, pid in pids.items()
            if self._process_exists(pid)
        }

    def _terminate_pids(self, pids: Mapping[str, int]) -> None:
        """Request termination for all known component PIDs."""
        for component, pid in sorted(
            pids.items(),
            key=lambda item: 1 if item[0] == "server" else 0,
        ):
            if pid > 0 and self._process_exists(pid):
                self._process_terminate(pid)

    def _kill_pids(self, pids: Mapping[str, int]) -> None:
        """Force termination for all known component PIDs."""
        for component, pid in sorted(
            pids.items(),
            key=lambda item: 1 if item[0] == "server" else 0,
        ):
            if pid > 0 and self._process_exists(pid):
                self._process_kill(pid)

    def _remove_state_files(self) -> None:
        """Remove PID and lock files."""
        self.paths.pid_file.unlink(missing_ok=True)
        self.paths.lock_file.unlink(missing_ok=True)

    def _status(
        self,
        state: str,
        message: str,
        pids: Mapping[str, int],
        command: Sequence[str],
        ports: Mapping[str, int | str | list[int]] | None = None,
        logs: Mapping[str, str] | None = None,
        components: Mapping[str, str] | None = None,
        worker_readiness: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> OrchestrationStatus:
        """Create an orchestration status object."""
        readiness = (
            {lane: dict(payload) for lane, payload in worker_readiness.items()}
            if worker_readiness is not None
            else self._worker_readiness_states(pids)
        )
        return OrchestrationStatus(
            state=state,
            message=message,
            state_dir=str(self.paths.state_dir),
            pid_file=str(self.paths.pid_file),
            lock_file=str(self.paths.lock_file),
            logs=dict(logs or self._default_logs()),
            pids=dict(pids),
            command=tuple(command),
            ports=dict(
                ports
                or cast(
                    dict[str, int | str | list[int]],
                    self.runtime_policy.ports.to_dict(),
                )
            ),
            components=dict(
                components or self._component_states(pids, readiness)
            ),
            worker_readiness=readiness,
            disk=_posix_disk_headroom(self.paths.runtime_dir),
        )

    def _path_dict(self) -> dict[str, str]:
        """Return path diagnostics."""
        return dict(self.paths.to_dict())


def _posix_disk_headroom(path: Path) -> dict[str, Any]:
    """Return POSIX write headroom for runtime state paths."""
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    usage = shutil.disk_usage(probe)
    return {
        "path": str(probe),
        "semantics": "posix_write_available",
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "note": (
            "This is filesystem space available to normal writes; desktop "
            "Finder availability may include purgeable space."
        ),
    }


RuntimeStatus = OrchestrationStatus
RuntimeSupervisor = OrchestrationSupervisor
build_worker_start_command = build_orchestration_worker_start_command

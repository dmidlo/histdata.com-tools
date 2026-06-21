"""Local Temporal sidecar process supervision."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, cast

from histdatacom.sidecar.resources import (
    current_platform_key,
    load_sidecar_manifest,
    read_sidecar_asset_text,
    sidecar_executable_path,
)
from histdatacom.sidecar.runtime import (
    PortAvailabilityProbe,
    SidecarPaths,
    SidecarRuntimePolicy,
    build_sidecar_runtime_policy,
    default_sidecar_state_dir,  # noqa:F401
    is_port_available,
)

SIDECAR_STATE_SCHEMA_VERSION = 1
DEFAULT_STARTUP_TIMEOUT_SECONDS = 10.0
DEFAULT_STOP_TIMEOUT_SECONDS = 10.0

ProcessFactory = Callable[..., Any]
ProcessExists = Callable[[int], bool]
ProcessTerminate = Callable[[int], None]


@dataclass(frozen=True, slots=True)
class SidecarStatus:
    """Serializable sidecar status for CLI, API, and future GUI callers."""

    state: str
    message: str
    state_dir: str
    pid_file: str
    lock_file: str
    logs: dict[str, str]
    pids: dict[str, int]
    command: tuple[str, ...] = ()
    ports: dict[str, int | str | list[int]] = field(default_factory=dict)

    @property
    def running(self) -> bool:
        """Return whether the sidecar is considered healthy enough to reuse."""
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
        }


def _utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _process_exists(pid: int) -> bool:
    """Return whether a process exists for a PID."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_process(pid: int) -> None:
    """Request process termination for a PID."""
    if pid <= 0:
        return
    os.kill(pid, signal.SIGTERM)


def _load_runtime_defaults() -> dict[str, Any]:
    """Load packaged runtime defaults used for command construction."""
    loaded = json.loads(read_sidecar_asset_text("runtime-defaults.json"))
    if not isinstance(loaded, dict):
        raise ValueError("runtime-defaults.json must contain an object")
    return cast(dict[str, Any], loaded)


def build_temporal_start_command(
    executable: Path | str,
    extra_args: Sequence[str] = (),
    *,
    runtime_policy: SidecarRuntimePolicy | None = None,
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


class SidecarSupervisor:
    """Supervise the local Temporal sidecar process group."""

    def __init__(
        self,
        paths: SidecarPaths | None = None,
        *,
        runtime_policy: SidecarRuntimePolicy | None = None,
        process_exists: ProcessExists = _process_exists,
        process_terminate: ProcessTerminate = _terminate_process,
        process_factory: ProcessFactory = subprocess.Popen,
        port_available: PortAvailabilityProbe = is_port_available,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Initialize the sidecar supervisor."""
        self.runtime_policy: SidecarRuntimePolicy = (
            runtime_policy or build_sidecar_runtime_policy(paths=paths)
        )
        self.paths: SidecarPaths = self.runtime_policy.paths
        self._process_exists = process_exists
        self._process_terminate = process_terminate
        self._process_factory = process_factory
        self._port_available = port_available
        self._sleep = sleep

    def status(self, *, repair: bool = False) -> SidecarStatus:
        """Return current sidecar process status."""
        if not self.paths.pid_file.exists():
            return self._status("stopped", "Sidecar is not running.", {}, ())

        try:
            state = self._read_state()
        except (OSError, ValueError, json.JSONDecodeError) as err:
            if repair:
                self._remove_state_files()
            return self._status(
                "stale",
                f"Sidecar state is unreadable: {err}",
                {},
                (),
            )

        pids = self._state_pids(state)
        command = tuple(str(item) for item in state.get("command", []))
        ports = self._state_ports(state)
        if not pids:
            if repair:
                self._remove_state_files()
            return self._status(
                "stale",
                "Sidecar state does not contain any valid process IDs.",
                {},
                command,
                ports,
            )

        missing = {
            component: pid
            for component, pid in pids.items()
            if not self._process_exists(pid)
        }
        if not missing:
            return self._status(
                "running",
                "Sidecar is running.",
                pids,
                command,
                ports,
            )

        if repair:
            self._remove_state_files()
        return self._status(
            "stale",
            f"Sidecar state references dead processes: {missing}.",
            pids,
            command,
            ports,
        )

    def start(
        self,
        *,
        executable: Path | str | None = None,
        extra_args: Sequence[str] = (),
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT_SECONDS,
    ) -> SidecarStatus:
        """Start the sidecar, or return running status if already healthy."""
        current = self.status(repair=True)
        if current.running:
            return self._status(
                "running",
                "Sidecar is already running.",
                current.pids,
                current.command,
            )

        self.paths.state_dir.mkdir(parents=True, exist_ok=True)
        runtime_policy = self.runtime_policy.with_available_ports(
            self._port_available
        )
        self.runtime_policy = runtime_policy
        self._acquire_lock()
        try:
            if executable is None:
                with sidecar_executable_path() as packaged_executable:
                    return self._start_process(
                        packaged_executable,
                        extra_args,
                        startup_timeout,
                        runtime_policy,
                    )
            return self._start_process(
                Path(executable).expanduser(),
                extra_args,
                startup_timeout,
                runtime_policy,
            )
        finally:
            self._release_lock()

    def stop(
        self,
        *,
        stop_timeout: float = DEFAULT_STOP_TIMEOUT_SECONDS,
    ) -> SidecarStatus:
        """Stop all known sidecar processes and remove persisted state."""
        self._acquire_lock()
        try:
            current = self.status(repair=False)
            if current.state == "stopped":
                return current
            if current.state == "stale":
                self._remove_state_files()
                return self._status(
                    "stopped",
                    "Removed stale sidecar state.",
                    {},
                    (),
                )

            for pid in current.pids.values():
                self._process_terminate(pid)
            deadline = time.monotonic() + stop_timeout
            while time.monotonic() < deadline:
                if not any(
                    self._process_exists(pid) for pid in current.pids.values()
                ):
                    break
                self._sleep(0.05)
            still_running = {
                component: pid
                for component, pid in current.pids.items()
                if self._process_exists(pid)
            }
            if still_running:
                return self._status(
                    "stopping",
                    f"Sidecar processes still running: {still_running}.",
                    still_running,
                    current.command,
                )
            self._remove_state_files()
            return self._status(
                "stopped",
                "Sidecar stopped.",
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
    ) -> SidecarStatus:
        """Restart the sidecar."""
        self.stop(stop_timeout=stop_timeout)
        return self.start(
            executable=executable,
            extra_args=extra_args,
            startup_timeout=startup_timeout,
        )

    def doctor(self) -> dict[str, Any]:
        """Return supervisor diagnostics without changing sidecar state."""
        status = self.status(repair=False)
        manifest = load_sidecar_manifest()
        platform_key = current_platform_key()
        platform_resource = manifest.platforms.get(platform_key)
        executable_bundled = (
            bool(platform_resource.bundled) if platform_resource else False
        )
        return {
            "status": status.to_dict(),
            "paths": self._path_dict(),
            "platform": {
                "key": platform_key,
                "supported": platform_resource is not None,
                "executable_bundled": executable_bundled,
                "message": (
                    "No packaged Temporal executable is available in this "
                    "artifact. Install a bundled platform wheel or pass "
                    "--executable."
                    if not executable_bundled
                    else "Packaged Temporal executable is available."
                ),
            },
            "runtime_defaults": _load_runtime_defaults(),
            "runtime_policy": self.runtime_policy.to_dict(),
        }

    def _start_process(
        self,
        executable: Path,
        extra_args: Sequence[str],
        startup_timeout: float,
        runtime_policy: SidecarRuntimePolicy,
    ) -> SidecarStatus:
        """Start the Temporal server process and persist state."""
        command = build_temporal_start_command(
            executable,
            extra_args,
            runtime_policy=runtime_policy,
        )
        runtime_policy.write_manifest()
        log = self.paths.server_log.open("ab")
        try:
            process = self._process_factory(
                list(command),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                close_fds=os.name != "nt",
                start_new_session=os.name != "nt",
            )
        except Exception:
            log.close()
            raise

        pid = int(process.pid)
        deadline = time.monotonic() + startup_timeout
        while time.monotonic() < deadline:
            poll = getattr(process, "poll", lambda: None)
            if poll() is None:
                state = {
                    "schema_version": SIDECAR_STATE_SCHEMA_VERSION,
                    "started_at_utc": _utc_now(),
                    "command": list(command),
                    "pids": {"server": pid},
                    "ports": runtime_policy.ports.to_dict(),
                    "runtime_policy": runtime_policy.to_dict(),
                    "logs": {
                        "server": str(self.paths.server_log),
                        "worker": str(self.paths.worker_log),
                    },
                }
                self._write_state(state)
                log.close()
                return self._status(
                    "running",
                    "Sidecar started.",
                    {"server": pid},
                    command,
                )
            self._sleep(0.05)

        log.close()
        raise RuntimeError("Temporal sidecar did not stay running at startup.")

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
                    f"Sidecar lock is held by live process {owner_pid}."
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
        """Read persisted sidecar process state."""
        loaded = json.loads(self.paths.pid_file.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("sidecar state must contain an object")
        return cast(dict[str, Any], loaded)

    def _write_state(self, state: Mapping[str, Any]) -> None:
        """Write persisted sidecar process state."""
        self.paths.pid_file.write_text(
            json.dumps(dict(state), indent=2, sort_keys=True),
            encoding="utf-8",
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
    ) -> SidecarStatus:
        """Create a sidecar status object."""
        return SidecarStatus(
            state=state,
            message=message,
            state_dir=str(self.paths.state_dir),
            pid_file=str(self.paths.pid_file),
            lock_file=str(self.paths.lock_file),
            logs={
                "server": str(self.paths.server_log),
                "worker": str(self.paths.worker_log),
            },
            pids=dict(pids),
            command=tuple(command),
            ports=dict(
                ports
                or cast(
                    dict[str, int | str | list[int]],
                    self.runtime_policy.ports.to_dict(),
                )
            ),
        )

    def _path_dict(self) -> dict[str, str]:
        """Return path diagnostics."""
        return dict(self.paths.to_dict())

"""Smoke-test an installed histdatacom package and runtime resources."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from importlib import metadata
from pathlib import Path
from typing import Any, Mapping, Sequence

DEFAULT_COMMAND_TIMEOUT_SECONDS = 120.0
MAX_DIAGNOSTIC_LOG_CHARS = 12000
MAX_DIAGNOSTIC_LOGS = 8
MAX_DIAGNOSTIC_STREAM_CHARS = 4000
WORKER_STARTUP_DIAGNOSTIC_LANES = (
    "orchestration",
    "network",
    "cpu-file",
    "influx",
)
WORKER_STARTUP_RUN_PROBE_SECONDS = 0.5
WORKER_COMMAND_RUN_PROBE_SECONDS = 5.0
WINDOWS_PARENT_INTERRUPT_MESSAGE = (
    "parent received KeyboardInterrupt while waiting for diagnostic subprocess; "
    "this usually means a Windows console-control event crossed a runtime "
    "process boundary"
)
EXPECTED_ASSETS = (
    "README.md",
    "manifest.json",
    "runtime-defaults.json",
    "temporal-runtime-index.json",
    "third-party/temporal-cli/LICENSE",
    "third-party/temporal-cli/NOTICE.md",
)
EXPECTED_CONSOLE_SCRIPTS = {
    "histdatacom": "histdatacom.histdata_com:main",
    "histdatacom-orchestration-worker": ("histdatacom.orchestration.worker:main"),
}
QUALITY_REPORT_SCHEMA_VERSION = "histdatacom.quality-report.v1"
QUALITY_SMOKE_CLEAN_ROWS = (
    "20120201 000003660,1.306600,1.306770,0",
    "20120201 000003973,1.306580,1.306750,25",
    "20120201 000014990,1.306570,1.306740,2147483647",
)
QUALITY_SMOKE_DIRTY_ROWS = (
    QUALITY_SMOKE_CLEAN_ROWS[0],
    "20120201 000003973,$1.306580,1.306750,25",
)


def _single_wheel(wheel_dir: Path) -> Path:
    """Return the only built histdatacom wheel in a directory."""
    wheels = sorted(wheel_dir.glob("histdatacom-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one wheel, found {wheels}")
    return wheels[0]


def _script_path(name: str) -> str:
    """Return an installed console script path."""
    script_path = shutil.which(name)
    if script_path is None:
        raise SystemExit(f"console script is not on PATH: {name}")
    return script_path


def _run(
    command: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    expected_returncodes: tuple[int, ...] = (0,),
    timeout: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    """Run a smoke command and fail with useful output when it breaks."""
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            check=False,
            env=(dict(env) if env is not None else None),
            text=True,
            timeout=timeout,
            **_windows_process_group_kwargs(),
        )
    except subprocess.TimeoutExpired as err:
        stdout = _command_output_text(err.stdout)
        stderr = _command_output_text(err.stderr)
        diagnostics = _runtime_log_diagnostics(command, stdout=stdout)
        raise SystemExit(
            f"command timed out after {timeout:g}s: {' '.join(command)}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
            f"{diagnostics}"
        ) from err
    if completed.returncode not in expected_returncodes:
        expected = ", ".join(str(code) for code in expected_returncodes)
        diagnostics = _runtime_log_diagnostics(
            command,
            stdout=completed.stdout,
        )
        raise SystemExit(
            f"command returned exit {completed.returncode}; expected "
            f"{expected}: "
            f"{' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
            f"{diagnostics}"
        )
    return completed


def _command_output_text(value: bytes | str | None) -> str:
    """Return subprocess timeout output as text."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _runtime_log_diagnostics(
    command: Sequence[str],
    *,
    stdout: str,
) -> str:
    """Return bounded runtime log excerpts for failed runtime commands."""
    log_dirs = _runtime_log_dirs(command, stdout=stdout)
    diagnostics: list[str] = []
    inspected_logs = 0
    for log_dir in log_dirs:
        if not log_dir.exists():
            diagnostics.append(f"\n--- runtime log dir missing: {log_dir} ---")
            continue
        if not log_dir.is_dir():
            diagnostics.append(
                f"\n--- runtime log path is not a directory: {log_dir} ---"
            )
            continue
        log_paths = sorted(log_dir.glob("*.log"))
        if not log_paths:
            diagnostics.append(f"\n--- runtime log dir empty: {log_dir} ---")
            continue
        for log_path in log_paths:
            if inspected_logs >= MAX_DIAGNOSTIC_LOGS:
                break
            inspected_logs += 1
            excerpt = _tail_text(log_path, limit=MAX_DIAGNOSTIC_LOG_CHARS)
            if not excerpt:
                diagnostics.append(f"\n--- runtime log empty: {log_path} ---")
                continue
            diagnostics.append(f"\n--- runtime log: {log_path} ---\n{excerpt}")
        if inspected_logs >= MAX_DIAGNOSTIC_LOGS:
            break
    if not diagnostics:
        return ""
    return "\nruntime log diagnostics:" + "".join(diagnostics)


def _runtime_log_dirs(
    command: Sequence[str],
    *,
    stdout: str,
) -> list[Path]:
    """Return candidate runtime log directories for a command failure."""
    candidates: list[Path] = []
    command_parts = list(command)
    for option in ("--state-dir", "--runtime-home"):
        for value in _option_values(command_parts, option):
            path = Path(value)
            if option == "--state-dir":
                runtime_dir = path.parent if path.name == "state" else path
                candidates.append(runtime_dir / "logs")
            else:
                candidates.append(path)

    stdout_payload = _json_object_or_none(stdout)
    if stdout_payload is not None:
        logs = stdout_payload.get("logs")
        if isinstance(logs, Mapping):
            for value in logs.values():
                if isinstance(value, str):
                    candidates.append(Path(value).parent)
        state_dir = stdout_payload.get("state_dir")
        if isinstance(state_dir, str):
            path = Path(state_dir)
            runtime_dir = path.parent if path.name == "state" else path
            candidates.append(runtime_dir / "logs")

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _option_values(command: Sequence[str], option: str) -> list[str]:
    """Return values that follow an option in a command sequence."""
    values: list[str] = []
    parts = list(command)
    for index, part in enumerate(parts):
        if part != option or index + 1 >= len(parts):
            continue
        values.append(parts[index + 1])
    return values


def _json_object_or_none(text: str) -> dict[str, Any] | None:
    """Return a JSON object parsed from text, if the text is one."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _tail_text(path: Path, *, limit: int) -> str:
    """Return a bounded text tail from a log path."""
    try:
        data = path.read_bytes()
    except OSError:
        return ""
    if not data:
        return ""
    tail = data[-limit:]
    text = tail.decode("utf-8", errors="replace")
    if len(data) > limit:
        return f"... <truncated to last {limit} bytes>\n{text}"
    return text


def _run_json(
    command: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Run a command that emits JSON and return the decoded payload."""
    completed = _run(command, env=env, timeout=timeout)
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as err:
        raise SystemExit(
            f"command did not emit valid JSON: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}"
        ) from err
    if not isinstance(payload, dict):
        raise SystemExit(f"command emitted non-object JSON: {' '.join(command)}")
    return payload


def _diagnostic_stream_text(text: str) -> str:
    """Return bounded command output for diagnostic JSON."""
    if len(text) <= MAX_DIAGNOSTIC_STREAM_CHARS:
        return text
    return (
        f"... <truncated to last {MAX_DIAGNOSTIC_STREAM_CHARS} chars>\n"
        f"{text[-MAX_DIAGNOSTIC_STREAM_CHARS:]}"
    )


def _run_diagnostic_command(
    phase: str,
    command: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Run a diagnostic command without hiding later blocking smoke output."""
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            check=False,
            env=(dict(env) if env is not None else None),
            text=True,
            timeout=timeout,
            **_windows_process_group_kwargs(),
        )
    except subprocess.TimeoutExpired as err:
        stdout = _command_output_text(err.stdout)
        stderr = _command_output_text(err.stderr)
        diagnostics = _runtime_log_diagnostics(command, stdout=stdout)
        return {
            "phase": phase,
            "status": "timed_out",
            "command": list(command),
            "timeout_seconds": timeout,
            "stdout": _diagnostic_stream_text(stdout),
            "stderr": _diagnostic_stream_text(stderr),
            "runtime_log_diagnostics": _diagnostic_stream_text(diagnostics),
        }
    except KeyboardInterrupt:
        if os.name != "nt":
            raise
        diagnostics = _runtime_log_diagnostics(command, stdout="")
        return {
            "phase": phase,
            "status": "interrupted",
            "command": list(command),
            "error_type": "KeyboardInterrupt",
            "message": WINDOWS_PARENT_INTERRUPT_MESSAGE,
            "runtime_log_diagnostics": _diagnostic_stream_text(diagnostics),
        }

    diagnostics = ""
    if completed.returncode != 0:
        diagnostics = _runtime_log_diagnostics(
            command,
            stdout=completed.stdout,
        )
    return {
        "phase": phase,
        "status": "passed" if completed.returncode == 0 else "failed",
        "command": list(command),
        "returncode": completed.returncode,
        "stdout": _diagnostic_stream_text(completed.stdout),
        "stderr": _diagnostic_stream_text(completed.stderr),
        "runtime_log_diagnostics": _diagnostic_stream_text(diagnostics),
    }


def _start_server_only_runtime(
    state_dir: Path,
    *,
    startup_timeout: float,
) -> tuple[dict[str, Any], Any | None]:
    """Start only the Temporal server for worker startup diagnostics."""
    try:
        from histdatacom.orchestration.runtime import (
            OrchestrationPaths,
            build_orchestration_runtime_policy,
        )
        from histdatacom.orchestration.supervisor import (
            OrchestrationSupervisor,
        )

        runtime_policy = build_orchestration_runtime_policy(
            paths=OrchestrationPaths.from_state_dir(state_dir),
        )
        supervisor = OrchestrationSupervisor(
            runtime_policy=runtime_policy,
            worker_lanes=(),
        )
        status = supervisor.start(startup_timeout=startup_timeout)
    except Exception as err:
        return (
            {
                "phase": "server_only_start",
                "status": "failed",
                "error_type": type(err).__name__,
                "message": str(err),
            },
            None,
        )

    payload = status.to_dict()
    phase = {
        "phase": "server_only_start",
        "status": "passed" if status.state == "running" else "failed",
        "payload": payload,
    }
    if status.state != "running":
        phase["message"] = status.message
    return phase, supervisor


def _stop_server_only_runtime(
    supervisor: Any,
    *,
    stop_timeout: float,
) -> dict[str, Any]:
    """Stop a server-only diagnostic runtime."""
    try:
        status = supervisor.stop(stop_timeout=stop_timeout)
    except KeyboardInterrupt:
        if os.name != "nt":
            raise
        return {
            "phase": "server_only_stop",
            "status": "interrupted",
            "error_type": "KeyboardInterrupt",
            "message": WINDOWS_PARENT_INTERRUPT_MESSAGE,
        }
    except Exception as err:
        return {
            "phase": "server_only_stop",
            "status": "failed",
            "error_type": type(err).__name__,
            "message": str(err),
        }

    payload = status.to_dict()
    phase = {
        "phase": "server_only_stop",
        "status": "passed" if status.state == "stopped" else "failed",
        "payload": payload,
    }
    if status.state != "stopped":
        phase["message"] = status.message
    return phase


def _windows_process_group_kwargs() -> dict[str, int]:
    """Return subprocess flags that isolate Windows console-control events."""
    if os.name != "nt":
        return {}
    return {
        "creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    }


def _server_only_runtime_env(supervisor: Any) -> dict[str, str]:
    """Return an environment that points worker diagnostics at the live server."""
    ports = supervisor.runtime_policy.ports
    env = dict(os.environ)
    env["HISTDATACOM_RUNTIME_IP"] = str(ports.bind_ip)
    env["HISTDATACOM_RUNTIME_PORT"] = str(ports.grpc)
    env["HISTDATACOM_RUNTIME_UI_PORT"] = str(ports.ui)
    return env


def _worker_startup_phase_name(prefix: str, lane: str) -> str:
    return f"{prefix}_{lane.replace('-', '_')}"


def _worker_startup_diagnostic_command(
    state_dir: Path,
    *,
    stop_after: str,
    lane: str | None = None,
) -> list[str]:
    """Build a phase-limited worker startup diagnostic command."""
    command = [
        sys.executable,
        "-m",
        "histdatacom.orchestration.worker",
        "--state-dir",
        str(state_dir),
        "--json",
        "diagnose-startup",
        "--stop-after",
        stop_after,
    ]
    if lane is not None:
        command.extend(["--lane", lane])
    if stop_after == "worker-run":
        command.extend(
            [
                "--run-probe-seconds",
                str(WORKER_STARTUP_RUN_PROBE_SECONDS),
            ]
        )
    return command


def _worker_run_command(supervisor: Any, lane: str) -> list[str]:
    """Build the exact supervised worker ``run`` command for a lane."""
    from histdatacom.orchestration.queues import build_orchestration_worker_config
    from histdatacom.orchestration.supervisor import (
        build_orchestration_worker_start_command,
    )

    config = build_orchestration_worker_config(
        runtime_policy=supervisor.runtime_policy,
        lane=lane,
    )
    return list(build_orchestration_worker_start_command(config))


def _run_worker_foreground_run_probe(
    phase: str,
    command: Sequence[str],
    *,
    timeout: float,
) -> dict[str, Any]:
    """Run the real worker ``run`` command in the foreground briefly."""
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
            **_windows_process_group_kwargs(),
        )
    except subprocess.TimeoutExpired as err:
        stdout = _command_output_text(err.stdout)
        stderr = _command_output_text(err.stderr)
        diagnostics = _runtime_log_diagnostics(command, stdout=stdout)
        return {
            "phase": phase,
            "status": "passed",
            "command": list(command),
            "message": "worker run command survived the startup probe",
            "timeout_seconds": timeout,
            "stdout": _diagnostic_stream_text(stdout),
            "stderr": _diagnostic_stream_text(stderr),
            "runtime_log_diagnostics": _diagnostic_stream_text(diagnostics),
        }
    except KeyboardInterrupt:
        if os.name != "nt":
            raise
        diagnostics = _runtime_log_diagnostics(command, stdout="")
        return {
            "phase": phase,
            "status": "interrupted",
            "command": list(command),
            "error_type": "KeyboardInterrupt",
            "message": WINDOWS_PARENT_INTERRUPT_MESSAGE,
            "runtime_log_diagnostics": _diagnostic_stream_text(diagnostics),
        }

    diagnostics = _runtime_log_diagnostics(command, stdout=completed.stdout)
    return {
        "phase": phase,
        "status": "failed",
        "command": list(command),
        "message": "worker run command exited before the startup probe elapsed",
        "returncode": completed.returncode,
        "stdout": _diagnostic_stream_text(completed.stdout),
        "stderr": _diagnostic_stream_text(completed.stderr),
        "runtime_log_diagnostics": _diagnostic_stream_text(diagnostics),
    }


def _run_worker_supervised_run_probe(
    phase: str,
    command: Sequence[str],
    state_dir: Path,
    *,
    lane: str,
    timeout: float,
) -> dict[str, Any]:
    """Launch a worker with supervisor-style ``Popen`` settings."""
    from histdatacom.orchestration.readiness import (
        read_worker_readiness,
        remove_worker_readiness,
    )

    log_path = state_dir / "logs" / f"{phase}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.unlink(missing_ok=True)
    remove_worker_readiness(state_dir, lane)

    process: subprocess.Popen[bytes] | None = None
    try:
        with log_path.open("ab") as log:
            process = subprocess.Popen(
                list(command),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                close_fds=True,
                start_new_session=os.name != "nt",
                **_windows_process_group_kwargs(),
            )
    except KeyboardInterrupt:
        if os.name != "nt":
            raise
        return {
            "phase": phase,
            "status": "interrupted",
            "command": list(command),
            "error_type": "KeyboardInterrupt",
            "message": WINDOWS_PARENT_INTERRUPT_MESSAGE,
            "log_path": str(log_path),
        }
    except Exception as err:
        return {
            "phase": phase,
            "status": "failed",
            "command": list(command),
            "error_type": type(err).__name__,
            "message": str(err),
            "log_path": str(log_path),
        }

    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            returncode = process.poll()
            if returncode is not None:
                log_tail = _tail_text(
                    log_path,
                    limit=MAX_DIAGNOSTIC_LOG_CHARS,
                )
                return {
                    "phase": phase,
                    "status": "failed",
                    "command": list(command),
                    "pid": int(process.pid),
                    "returncode": returncode,
                    "message": (
                        "supervisor-style worker process exited before "
                        "readiness"
                    ),
                    "log_path": str(log_path),
                    "log_tail": _diagnostic_stream_text(log_tail),
                }

            readiness = read_worker_readiness(state_dir, lane)
            if (
                readiness is not None
                and int(readiness.get("pid", 0) or 0) == int(process.pid)
                and readiness.get("state") == "ready"
            ):
                log_tail = _tail_text(
                    log_path,
                    limit=MAX_DIAGNOSTIC_LOG_CHARS,
                )
                return {
                    "phase": phase,
                    "status": "passed",
                    "command": list(command),
                    "pid": int(process.pid),
                    "message": (
                        "supervisor-style worker process wrote readiness"
                    ),
                    "readiness": readiness,
                    "log_path": str(log_path),
                    "log_tail": _diagnostic_stream_text(log_tail),
                }

            time.sleep(0.05)

        log_tail = _tail_text(log_path, limit=MAX_DIAGNOSTIC_LOG_CHARS)
        return {
            "phase": phase,
            "status": "timed_out",
            "command": list(command),
            "pid": int(process.pid),
            "message": "supervisor-style worker process did not become ready",
            "timeout_seconds": timeout,
            "log_path": str(log_path),
            "log_tail": _diagnostic_stream_text(log_tail),
        }
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5.0)


def _diagnostic_phase_passed(
    phases: Mapping[str, Mapping[str, Any]],
    name: str,
) -> bool:
    return phases.get(name, {}).get("status") == "passed"


def _runtime_startup_diagnostic_checks() -> tuple[tuple[str, str], ...]:
    checks = [
        ("python_import", "python_import"),
        ("temporalio_bridge", "temporalio_bridge"),
        ("histdatacom_console", "console_script_startup"),
        ("worker_console", "console_script_startup"),
        ("runtime_worker_start", "runtime_worker_startup"),
        ("runtime_stop", "runtime_shutdown"),
        ("server_only_start", "temporal_server_startup"),
        ("worker_client_connect", "temporal_client_connect"),
    ]
    for lane in WORKER_STARTUP_DIAGNOSTIC_LANES:
        checks.append(
            (
                _worker_startup_phase_name("worker_construct", lane),
                "temporal_worker_construction",
            )
        )
        checks.append(
            (
                _worker_startup_phase_name("worker_run_probe", lane),
                "temporal_worker_run_start",
            )
        )
    checks.append(
        (
            _worker_startup_phase_name(
                "worker_foreground_run_probe",
                "orchestration",
            ),
            "temporal_worker_run_command",
        )
    )
    checks.append(
        (
            _worker_startup_phase_name(
                "worker_supervised_run_probe",
                "orchestration",
            ),
            "supervised_worker_process_boundary",
        )
    )
    checks.extend(
        [
            ("server_only_stop", "temporal_server_shutdown"),
        ]
    )
    return tuple(checks)


def _runtime_startup_diagnostic_summary(
    phases: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    """Return the first failing startup layer for release-run triage."""
    for phase_name, layer in _runtime_startup_diagnostic_checks():
        if not _diagnostic_phase_passed(phases, phase_name):
            if phase_name == "runtime_worker_start":
                return _runtime_worker_failure_summary(phases)
            return _diagnostic_failure_summary(phases, phase_name, layer)
    return {
        "layer": "runtime_startup",
        "phase": "runtime_worker_start",
        "status": "passed",
    }


def _diagnostic_failure_summary(
    phases: Mapping[str, Mapping[str, Any]],
    phase_name: str,
    layer: str,
) -> dict[str, str]:
    phase = phases.get(phase_name, {})
    summary = {
        "layer": layer,
        "phase": phase_name,
        "status": str(phase.get("status", "failed")),
    }
    lane = _diagnostic_lane_from_phase(phase_name)
    if lane:
        summary["lane"] = lane
    if _phase_has_windows_native_startup_crash(phase) and phase_name.startswith(
        (
            "worker_construct_",
            "worker_run_probe_",
            "worker_foreground_run_probe_",
            "worker_supervised_run_probe_",
        )
    ):
        summary["layer"] = "temporalio_nexus_native_worker_initialization"
    return summary


def _diagnostic_lane_from_phase(phase_name: str) -> str:
    for lane in WORKER_STARTUP_DIAGNOSTIC_LANES:
        if phase_name in {
            _worker_startup_phase_name("worker_construct", lane),
            _worker_startup_phase_name("worker_run_probe", lane),
            _worker_startup_phase_name("worker_foreground_run_probe", lane),
            _worker_startup_phase_name("worker_supervised_run_probe", lane),
        }:
            return lane
    return ""


def _phase_has_windows_native_startup_crash(
    phase: Mapping[str, Any],
) -> bool:
    detail = "\n".join(
        str(phase.get(key, ""))
        for key in (
            "returncode",
            "stdout",
            "stderr",
            "payload",
            "runtime_log_diagnostics",
            "message",
        )
    ).lower()
    return "3221225794" in detail or "0xc0000142" in detail


def _runtime_worker_failure_summary(
    phases: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    """Classify worker-start failures after import and console checks pass."""
    phase = phases.get("runtime_worker_start", {})
    if _phase_has_windows_native_startup_crash(phase):
        return {
            "layer": "supervised_worker_process_boundary",
            "phase": "runtime_worker_start",
            "status": str(phase.get("status", "failed")),
        }
    return {
        "layer": "runtime_worker_startup",
        "phase": "runtime_worker_start",
        "status": str(phase.get("status", "failed")),
    }


def _mark_runtime_start_payload(
    phase: dict[str, Any],
) -> dict[str, Any] | None:
    """Attach parsed runtime start JSON and fail non-running payloads."""
    payload = _json_object_or_none(str(phase.get("stdout", "")))
    if payload is not None:
        phase["payload"] = payload
    if phase.get("status") == "passed" and payload is None:
        phase["status"] = "failed"
        phase["message"] = "runtime start did not emit JSON"
    elif phase.get("status") == "passed" and payload.get("state") != "running":
        phase["status"] = "failed"
        phase["message"] = "runtime start did not report running state"
    return payload


def _run_full_runtime_start_stop_diagnostic(
    phases: dict[str, dict[str, Any]],
    histdatacom: str,
    state_dir: Path,
    *,
    startup_timeout: float,
    stop_timeout: float,
) -> None:
    """Run the real supervised runtime path before lower-level probes."""
    runtime_start = _run_diagnostic_command(
        "runtime_worker_start",
        [
            histdatacom,
            "runtime",
            "--state-dir",
            str(state_dir),
            "--json",
            "start",
            "--startup-timeout",
            str(startup_timeout),
        ],
        timeout=startup_timeout + 60.0,
    )
    _mark_runtime_start_payload(runtime_start)
    phases["runtime_worker_start"] = runtime_start
    phases["runtime_stop"] = _run_diagnostic_command(
        "runtime_stop",
        [
            histdatacom,
            "runtime",
            "--state-dir",
            str(state_dir),
            "--json",
            "stop",
        ],
        timeout=stop_timeout + 60.0,
    )


def check_windows_runtime_diagnostic(
    state_dir: Path,
    *,
    startup_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Collect a Windows release-smoke startup diagnostic by layer."""
    diagnostic_state_dir = state_dir.parent / f"{state_dir.name}-windows-diagnostic"
    runtime_state_dir = state_dir.parent / f"{state_dir.name}-windows-runtime"
    diagnostic_state_dir.mkdir(parents=True, exist_ok=True)
    runtime_state_dir.mkdir(parents=True, exist_ok=True)
    histdatacom = _script_path("histdatacom")
    worker = _script_path("histdatacom-orchestration-worker")
    phases: dict[str, dict[str, Any]] = {}

    phases["python_import"] = _run_diagnostic_command(
        "python_import",
        [
            sys.executable,
            "-c",
            (
                "import json, sys; "
                "import histdatacom; "
                "import histdatacom.orchestration.supervisor; "
                "import histdatacom.orchestration.worker; "
                "print(json.dumps({"
                "'python': sys.version.split()[0], "
                "'executable': sys.executable, "
                "'histdatacom': histdatacom.__version__"
                "}))"
            ),
        ],
    )
    phases["temporalio_bridge"] = _run_diagnostic_command(
        "temporalio_bridge",
        [
            sys.executable,
            "-c",
            (
                "import importlib.metadata, json; "
                "import temporalio, temporalio.bridge, "
                "temporalio.client, temporalio.worker; "
                "print(json.dumps({"
                "'temporalio': importlib.metadata.version('temporalio'), "
                "'bridge': temporalio.bridge.__name__"
                "}))"
            ),
        ],
    )
    phases["histdatacom_console"] = _run_diagnostic_command(
        "histdatacom_console",
        [histdatacom, "--version"],
    )
    phases["worker_console"] = _run_diagnostic_command(
        "worker_console",
        [worker, "--help"],
    )

    if all(
        _diagnostic_phase_passed(phases, phase_name)
        for phase_name in (
            "python_import",
            "temporalio_bridge",
            "histdatacom_console",
            "worker_console",
        )
    ):
        _run_full_runtime_start_stop_diagnostic(
            phases,
            histdatacom,
            runtime_state_dir,
            startup_timeout=startup_timeout,
            stop_timeout=stop_timeout,
        )

        server_start, server_supervisor = _start_server_only_runtime(
            diagnostic_state_dir,
            startup_timeout=startup_timeout,
        )
        phases["server_only_start"] = server_start
        try:
            if (
                _diagnostic_phase_passed(phases, "server_only_start")
                and server_supervisor is not None
            ):
                worker_env = _server_only_runtime_env(server_supervisor)
                phases["worker_client_connect"] = _run_diagnostic_command(
                    "worker_client_connect",
                    _worker_startup_diagnostic_command(
                        diagnostic_state_dir,
                        stop_after="client-connect",
                    ),
                    env=worker_env,
                    timeout=startup_timeout + 60.0,
                )
                for lane in WORKER_STARTUP_DIAGNOSTIC_LANES:
                    construct_phase = _worker_startup_phase_name(
                        "worker_construct",
                        lane,
                    )
                    phases[construct_phase] = _run_diagnostic_command(
                        construct_phase,
                        _worker_startup_diagnostic_command(
                            diagnostic_state_dir,
                            stop_after="worker-construct",
                            lane=lane,
                        ),
                        env=worker_env,
                        timeout=startup_timeout + 60.0,
                    )
                    run_phase = _worker_startup_phase_name(
                        "worker_run_probe",
                        lane,
                    )
                    phases[run_phase] = _run_diagnostic_command(
                        run_phase,
                        _worker_startup_diagnostic_command(
                            diagnostic_state_dir,
                            stop_after="worker-run",
                            lane=lane,
                        ),
                        env=worker_env,
                        timeout=startup_timeout + 60.0,
                    )

                exact_run_command = _worker_run_command(
                    server_supervisor,
                    "orchestration",
                )
                phases[
                    _worker_startup_phase_name(
                        "worker_foreground_run_probe",
                        "orchestration",
                    )
                ] = _run_worker_foreground_run_probe(
                    _worker_startup_phase_name(
                        "worker_foreground_run_probe",
                        "orchestration",
                    ),
                    exact_run_command,
                    timeout=WORKER_COMMAND_RUN_PROBE_SECONDS,
                )
                phases[
                    _worker_startup_phase_name(
                        "worker_supervised_run_probe",
                        "orchestration",
                    )
                ] = _run_worker_supervised_run_probe(
                    _worker_startup_phase_name(
                        "worker_supervised_run_probe",
                        "orchestration",
                    ),
                    exact_run_command,
                    diagnostic_state_dir,
                    lane="orchestration",
                    timeout=startup_timeout,
                )
        finally:
            if server_supervisor is not None:
                phases["server_only_stop"] = _stop_server_only_runtime(
                    server_supervisor,
                    stop_timeout=stop_timeout,
                )

    report = {
        "os_name": os.name,
        "platform": sys.platform,
        "python_executable": sys.executable,
        "state_dir": str(diagnostic_state_dir),
        "runtime_state_dir": str(runtime_state_dir),
        "console_scripts": {
            "histdatacom": histdatacom,
            "histdatacom-orchestration-worker": worker,
        },
        "summary": _runtime_startup_diagnostic_summary(phases),
        "phases": phases,
    }
    print(  # noqa:T201
        f"windows runtime diagnostic: {json.dumps(report, sort_keys=True)}",
        flush=True,
    )
    return report


def install_wheel(
    *,
    wheel_dir: Path | None = None,
    wheel_path: Path | None = None,
    install_temporal_extra: bool = False,
) -> Path:
    """Install the built wheel into the active Python environment."""
    resolved_wheel = wheel_path or _single_wheel(wheel_dir or Path("dist"))
    install_target = str(resolved_wheel)
    if install_temporal_extra:
        install_target = f"histdatacom[temporal] @ {resolved_wheel.resolve().as_uri()}"
    subprocess.check_call([sys.executable, "-m", "pip", "install", install_target])
    return resolved_wheel


def check_package_metadata(*, expect_temporal_extra: bool) -> dict[str, Any]:
    """Validate installed package metadata and console entry points."""
    import histdatacom
    from histdatacom.orchestration.contracts import RunRequest
    from histdatacom.runtime_contracts import RunRequest as RuntimeRunRequest

    dist = metadata.distribution("histdatacom")
    scripts = {
        entry.name: entry.value
        for entry in metadata.entry_points().select(group="console_scripts")
    }
    for script_name, expected_target in EXPECTED_CONSOLE_SCRIPTS.items():
        actual_target = scripts.get(script_name)
        if actual_target != expected_target:
            raise SystemExit(
                f"{script_name} entry point expected {expected_target!r}, "
                f"found {actual_target!r}"
            )
        _script_path(script_name)

    installed_version = metadata.version("histdatacom")
    if installed_version != histdatacom.__version__:
        raise SystemExit(
            "installed package metadata version does not match imported "
            f"package: {installed_version!r} != {histdatacom.__version__!r}"
        )
    if dist.metadata["Name"] != "histdatacom":
        raise SystemExit(f"unexpected installed name: {dist.metadata['Name']}")
    provides_extra = set(dist.metadata.get_all("Provides-Extra", []))
    if expect_temporal_extra and "temporal" not in provides_extra:
        raise SystemExit("temporal compatibility extra missing from metadata")

    temporalio_version = metadata.version("temporalio")
    if importlib.util.find_spec("temporalio") is None:
        raise SystemExit("temporalio distribution is installed but missing")
    tzdata_version = metadata.version("tzdata")
    if importlib.util.find_spec("tzdata") is None:
        raise SystemExit("tzdata distribution is installed but missing")
    if RunRequest is not RuntimeRunRequest:
        raise SystemExit(
            "orchestration contract RunRequest does not match runtime contract"
        )

    return {
        "name": dist.metadata["Name"],
        "version": installed_version,
        "console_scripts": sorted(EXPECTED_CONSOLE_SCRIPTS),
        "orchestration_contracts": ["RunRequest"],
        "temporalio_version": temporalio_version,
        "tzdata_version": tzdata_version,
    }


def check_runtime_resources(
    *,
    require_bundled_current_platform: bool = False,
    require_external_runtime_provisioning: bool = False,
    check_executable_version: bool = False,
    temporal_executable: Path | None = None,
) -> dict[str, Any]:
    """Validate installed runtime resources for the current platform."""
    from histdatacom.orchestration.resources import (
        TemporalExecutableUnavailable,
        current_platform_key,
        inspect_temporal_runtime_cache,
        load_runtime_manifest,
        load_temporal_runtime_index,
        packaged_temporal_executable_path,
        runtime_asset,
        temporal_runtime_executable_path,
    )

    manifest = load_runtime_manifest()
    runtime_index = load_temporal_runtime_index(manifest)
    for asset in EXPECTED_ASSETS:
        if not runtime_asset(asset).is_file():
            raise SystemExit(f"runtime asset is not a file: {asset}")

    platform_key = current_platform_key()
    platform_resource = manifest.platforms.get(platform_key)
    platform_artifact = runtime_index.platforms.get(platform_key)
    if platform_resource is None:
        supported = ", ".join(sorted(manifest.platforms))
        raise SystemExit(
            f"current platform {platform_key!r} is not declared in runtime "
            f"manifest. Supported platforms: {supported}"
        )
    executable_version = ""
    resolver_source = ""
    resolver_network_fetch = False
    runtime_resolution: dict[str, Any] | None = None
    if require_external_runtime_provisioning and platform_resource.bundled:
        raise SystemExit(
            "external runtime provisioning was required, but the installed "
            f"wheel bundles a Temporal executable for {platform_key!r}"
        )
    if require_external_runtime_provisioning and not check_executable_version:
        raise SystemExit(
            "--require-external-runtime-provisioning must be combined with "
            "--check-executable-version so the resolver is exercised"
        )
    if platform_resource.bundled:
        with packaged_temporal_executable_path(platform_key) as executable_path:
            if not executable_path.is_file():
                raise SystemExit(
                    f"bundled runtime executable is missing: {executable_path}"
                )
            if check_executable_version:
                with temporal_runtime_executable_path(
                    explicit_executable=temporal_executable,
                ) as resolution:
                    completed = _run([str(resolution.executable), "--version"])
                    runtime_resolution = resolution.to_dict()
                    resolver_source = resolution.source
                    resolver_network_fetch = resolution.network_fetch
                executable_version = (
                    completed.stdout.strip() or completed.stderr.strip()
                )
    else:
        if require_bundled_current_platform:
            raise SystemExit(
                f"current platform {platform_key!r} is not bundled in this wheel"
            )
        try:
            with packaged_temporal_executable_path(platform_key):
                raise SystemExit("metadata-only runtime resource exposed an executable")
        except TemporalExecutableUnavailable as err:
            if "not bundled in this distribution" not in str(err):
                raise
        if check_executable_version:
            with temporal_runtime_executable_path(
                explicit_executable=temporal_executable,
            ) as resolution:
                completed = _run([str(resolution.executable), "--version"])
                executable_version = (
                    completed.stdout.strip() or completed.stderr.strip()
                )
                runtime_resolution = resolution.to_dict()
                resolver_source = resolution.source
                resolver_network_fetch = resolution.network_fetch

    if require_external_runtime_provisioning and resolver_source not in {
        "cache",
        "download",
    }:
        raise SystemExit(
            "external runtime provisioning must resolve from the pinned cache "
            "or first-run download path, not "
            f"{resolver_source or 'an unexercised resolver'}"
        )

    return {
        "runtime": manifest.runtime,
        "distribution_strategy": manifest.distribution_strategy,
        "runtime_index_version": runtime_index.version,
        "embedded_binary": manifest.embedded_binary,
        "platform": platform_key,
        "platform_bundled": platform_resource.bundled,
        "platform_artifact": (
            {
                "archive_name": platform_artifact.archive_name,
                "archive_sha256": platform_artifact.archive_sha256,
                "archive_size_bytes": platform_artifact.archive_size_bytes,
            }
            if platform_artifact is not None
            else None
        ),
        "runtime_cache_entries": [
            entry.to_dict()
            for entry in inspect_temporal_runtime_cache()
            if entry.platform_key == platform_key
        ],
        "runtime_resolution": runtime_resolution,
        "resolver_source": resolver_source,
        "resolver_network_fetch": resolver_network_fetch,
        "executable_version": executable_version,
    }


def check_cli_smoke(
    state_dir: Path,
    *,
    require_bundled_current_platform: bool = False,
    start_runtime: bool = False,
    startup_timeout: float = 20.0,
    stop_timeout: float = 30.0,
) -> dict[str, Any]:
    """Run offline CLI smoke checks against a temporary runtime state dir."""
    state_dir.mkdir(parents=True, exist_ok=True)
    histdatacom = _script_path("histdatacom")
    worker = _script_path("histdatacom-orchestration-worker")
    _run([histdatacom, "--version"])
    _run([worker, "--help"])

    status = _run_json(
        [
            histdatacom,
            "runtime",
            "--state-dir",
            str(state_dir),
            "--json",
            "status",
        ]
    )
    if status.get("state") not in {"running", "stopped"}:
        raise SystemExit(f"unexpected runtime status payload: {status}")

    doctor = _run_json(
        [
            histdatacom,
            "runtime",
            "--state-dir",
            str(state_dir),
            "--json",
            "doctor",
        ]
    )
    platform = doctor.get("platform", {})
    if not isinstance(platform, dict) or not platform.get("supported"):
        raise SystemExit(f"unexpected runtime doctor payload: {doctor}")
    if (
        require_bundled_current_platform
        and platform.get("executable_bundled") is not True
    ):
        raise SystemExit(
            "runtime doctor did not report a bundled current-platform "
            f"executable: {doctor}"
        )

    start_state = ""
    stop_state = ""
    if start_runtime:
        start = _run_json(
            [
                histdatacom,
                "runtime",
                "--state-dir",
                str(state_dir),
                "--json",
                "start",
                "--startup-timeout",
                str(startup_timeout),
            ],
            timeout=startup_timeout + 60.0,
        )
        start_state = str(start.get("state", ""))
        if start_state != "running":
            raise SystemExit(f"unexpected runtime start payload: {start}")
        stop = _run_json(
            [
                histdatacom,
                "runtime",
                "--state-dir",
                str(state_dir),
                "--json",
                "stop",
            ],
            timeout=stop_timeout + 60.0,
        )
        stop_state = str(stop.get("state", ""))
    return {
        "status_state": status["state"],
        "doctor_supported": platform["supported"],
        "doctor_executable_bundled": platform.get("executable_bundled"),
        "start_state": start_state,
        "stop_state": stop_state,
    }


def check_live_runtime_smoke(
    *,
    workspace: Path,
    runtime_home: Path,
    data_directory: Path,
    temporal_executable: Path | None = None,
    startup_timeout: float,
    completion_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Run an external HistData.com runtime smoke."""
    from histdatacom.orchestration.live_smoke import (
        LiveOrchestrationSmokeError,
        diagnostics_json,
        run_live_orchestration_smoke,
    )

    try:
        return dict(
            run_live_orchestration_smoke(
                workspace=workspace,
                runtime_home=runtime_home,
                data_directory=data_directory,
                temporal_executable=temporal_executable,
                startup_timeout=startup_timeout,
                completion_timeout=completion_timeout,
                stop_timeout=stop_timeout,
            ).to_dict()
        )
    except LiveOrchestrationSmokeError as err:
        raise SystemExit(
            "live runtime smoke failed with diagnostics:\n"
            f"{diagnostics_json(err.diagnostics)}"
        ) from err


def check_hermetic_runtime_smoke(
    *,
    workspace: Path,
    runtime_home: Path,
    data_directory: Path,
    temporal_executable: Path | None = None,
    startup_timeout: float,
    completion_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Run a local-only Temporal runtime smoke for installed wheels."""
    from histdatacom.orchestration.live_smoke import (
        LiveOrchestrationSmokeError,
        diagnostics_json,
        run_hermetic_orchestration_smoke,
    )

    try:
        return dict(
            run_hermetic_orchestration_smoke(
                workspace=workspace,
                runtime_home=runtime_home,
                data_directory=data_directory,
                temporal_executable=temporal_executable,
                startup_timeout=startup_timeout,
                completion_timeout=completion_timeout,
                stop_timeout=stop_timeout,
            ).to_dict()
        )
    except LiveOrchestrationSmokeError as err:
        raise SystemExit(
            "hermetic runtime smoke failed with diagnostics:\n"
            f"{diagnostics_json(err.diagnostics)}"
        ) from err


def check_default_routing_runtime_smoke(
    *,
    workspace: Path,
    runtime_home: Path,
    data_directory: Path,
    temporal_executable: Path | None = None,
    startup_timeout: float,
    completion_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Run local-only smoke through default client routing."""
    from histdatacom.orchestration.live_smoke import (
        LiveOrchestrationSmokeError,
        diagnostics_json,
        run_default_client_routing_orchestration_smoke,
    )

    try:
        return dict(
            run_default_client_routing_orchestration_smoke(
                workspace=workspace,
                runtime_home=runtime_home,
                data_directory=data_directory,
                temporal_executable=temporal_executable,
                startup_timeout=startup_timeout,
                completion_timeout=completion_timeout,
                stop_timeout=stop_timeout,
            ).to_dict()
        )
    except LiveOrchestrationSmokeError as err:
        raise SystemExit(
            "default-routing runtime smoke failed with diagnostics:\n"
            f"{diagnostics_json(err.diagnostics)}"
        ) from err


def check_quality_runtime_smoke(
    *,
    workspace: Path,
    runtime_home: Path,
    data_directory: Path,
    temporal_executable: Path | None = None,
    startup_timeout: float,
    stop_timeout: float,
) -> dict[str, Any]:
    """Run installed CLI quality checks through the packaged runtime."""
    smoke_env = _quality_runtime_env(
        workspace=workspace,
        runtime_home=runtime_home,
    )
    fixtures = _write_quality_smoke_fixtures(data_directory)
    report_dir = data_directory / "quality-smoke-reports"
    clean_report = report_dir / "quality-clean.json"
    dirty_report = report_dir / "quality-dirty.json"
    start_payload: dict[str, Any] | None = None
    stop_payload: dict[str, Any] | None = None
    try:
        start_payload = _start_quality_runtime(
            workspace=workspace,
            runtime_home=runtime_home,
            temporal_executable=temporal_executable,
            startup_timeout=startup_timeout,
            env=smoke_env,
        )
        clean = _run_quality_cli(
            target=fixtures["clean"],
            report=clean_report,
            data_directory=data_directory,
            env=smoke_env,
            expected_returncodes=(0,),
        )
        dirty = _run_quality_cli(
            target=fixtures["dirty"],
            report=dirty_report,
            data_directory=data_directory,
            env=smoke_env,
            expected_returncodes=(1,),
        )
        clean_payload = _validate_quality_report(
            clean_report,
            expected_status="clean",
            min_errors=0,
            max_errors=0,
        )
        dirty_payload = _validate_quality_report(
            dirty_report,
            expected_status="failed",
            min_errors=1,
            max_errors=None,
        )
        jobs_payload = _quality_jobs_payload(
            workspace=workspace,
            runtime_home=runtime_home,
            env=smoke_env,
        )
        jobs = _validate_quality_runtime_jobs(
            jobs_payload,
            clean_target=fixtures["clean"],
            clean_report=clean_report,
            dirty_target=fixtures["dirty"],
            dirty_report=dirty_report,
        )
    finally:
        stop_payload = _stop_quality_runtime(
            workspace=workspace,
            runtime_home=runtime_home,
            stop_timeout=stop_timeout,
            env=smoke_env,
        )
        _validate_quality_runtime_stop(stop_payload)

    return {
        "start_state": str((start_payload or {}).get("state", "")),
        "stop_state": str((stop_payload or {}).get("state", "")),
        "clean": _quality_smoke_case_result(
            completed=clean,
            report_path=clean_report,
            payload=clean_payload,
        ),
        "dirty": _quality_smoke_case_result(
            completed=dirty,
            report_path=dirty_report,
            payload=dirty_payload,
        ),
        "jobs": jobs,
    }


def _quality_runtime_env(
    *,
    workspace: Path,
    runtime_home: Path,
) -> dict[str, str]:
    env = dict(os.environ)
    env["HISTDATACOM_RUNTIME_WORKSPACE"] = str(workspace)
    env["HISTDATACOM_RUNTIME_HOME"] = str(runtime_home)
    return env


def _write_quality_smoke_fixtures(data_directory: Path) -> dict[str, Path]:
    fixture_dir = data_directory / "quality-smoke-fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    clean = fixture_dir / "DAT_ASCII_EURUSD_T_201202.csv"
    dirty = fixture_dir / "DAT_ASCII_EURUSD_T_201202_BAD_NUMERIC.csv"
    clean.write_text(
        "\n".join(QUALITY_SMOKE_CLEAN_ROWS) + "\n",
        encoding="ascii",
    )
    dirty.write_text(
        "\n".join(QUALITY_SMOKE_DIRTY_ROWS) + "\n",
        encoding="ascii",
    )
    return {"clean": clean, "dirty": dirty}


def _start_quality_runtime(
    *,
    workspace: Path,
    runtime_home: Path,
    temporal_executable: Path | None,
    startup_timeout: float,
    env: Mapping[str, str],
) -> dict[str, Any]:
    command = [
        _script_path("histdatacom"),
        "runtime",
        "--workspace",
        str(workspace),
        "--runtime-home",
        str(runtime_home),
        "--json",
        "start",
        "--startup-timeout",
        str(startup_timeout),
    ]
    if temporal_executable is not None:
        command.extend(["--executable", str(temporal_executable)])
    payload = _run_json(command, env=env, timeout=startup_timeout + 60.0)
    if payload.get("state") != "running":
        raise SystemExit(f"quality runtime did not start: {payload}")
    return payload


def _run_quality_cli(
    *,
    target: Path,
    report: Path,
    data_directory: Path,
    env: Mapping[str, str],
    expected_returncodes: tuple[int, ...],
) -> subprocess.CompletedProcess[str]:
    return _run(
        [
            _script_path("histdatacom"),
            "--no-orchestration-start",
            "--data-directory",
            str(data_directory),
            "--quality",
            "--quality-target",
            str(target),
            "--quality-checks",
            "ingestion",
            "--quality-report",
            str(report),
        ],
        env=env,
        expected_returncodes=expected_returncodes,
    )


def _validate_quality_report(
    path: Path,
    *,
    expected_status: str,
    min_errors: int,
    max_errors: int | None,
) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as err:
        raise SystemExit(f"quality report was not written: {path}") from err
    except json.JSONDecodeError as err:
        raise SystemExit(f"quality report is invalid JSON: {path}") from err
    if not isinstance(payload, dict):
        raise SystemExit(f"quality report is not a JSON object: {path}")
    if payload.get("schema_version") != QUALITY_REPORT_SCHEMA_VERSION:
        raise SystemExit(f"quality report has unexpected schema version: {path}")
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise SystemExit(f"quality report missing summary: {path}")
    if summary.get("target_count") != 1:
        raise SystemExit(f"quality report expected one target: {path} {summary}")
    if summary.get("status") != expected_status:
        raise SystemExit(
            "quality report had unexpected status: "
            f"{path} expected={expected_status} summary={summary}"
        )
    error_count = int(summary.get("error_count", 0) or 0)
    if error_count < min_errors:
        raise SystemExit(
            f"quality report expected at least {min_errors} errors: {path} {summary}"
        )
    if max_errors is not None and error_count > max_errors:
        raise SystemExit(
            f"quality report expected at most {max_errors} errors: {path} {summary}"
        )
    if not payload.get("target_summaries"):
        raise SystemExit(f"quality report missing target summaries: {path}")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict) or metadata.get("operation") != ("data-quality"):
        raise SystemExit(f"quality report missing operation metadata: {path}")
    return payload


def _quality_jobs_payload(
    *,
    workspace: Path,
    runtime_home: Path,
    env: Mapping[str, str],
) -> dict[str, Any]:
    return _run_json(
        [
            _script_path("histdatacom"),
            "jobs",
            "--workspace",
            str(workspace),
            "--runtime-home",
            str(runtime_home),
            "--json",
            "list",
            "--offline",
        ],
        env=env,
    )


def _validate_quality_runtime_jobs(
    jobs_payload: Mapping[str, Any],
    *,
    clean_target: Path,
    clean_report: Path,
    dirty_target: Path,
    dirty_report: Path,
) -> dict[str, Any]:
    jobs = jobs_payload.get("jobs")
    if not isinstance(jobs, list):
        raise SystemExit(f"runtime jobs payload missing jobs: {jobs_payload}")
    clean_job = _find_quality_job(jobs, clean_target, clean_report)
    dirty_job = _find_quality_job(jobs, dirty_target, dirty_report)
    _validate_quality_job(clean_job, expected_status="completed")
    _validate_quality_job(dirty_job, expected_status="failed")
    return {
        "count": len(jobs),
        "clean_workflow_id": str(clean_job.get("workflow_id", "")),
        "dirty_workflow_id": str(dirty_job.get("workflow_id", "")),
        "clean_status": _normalized_quality_job_status(clean_job),
        "dirty_status": _normalized_quality_job_status(dirty_job),
    }


def _find_quality_job(
    jobs: Sequence[Any],
    target: Path,
    report: Path,
) -> Mapping[str, Any]:
    expected_target = str(target)
    expected_report = str(report)
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        metadata = job.get("metadata")
        if not isinstance(metadata, Mapping):
            continue
        request = metadata.get("run_request")
        if not isinstance(request, Mapping):
            continue
        if request.get("data_quality") is not True:
            continue
        if expected_target not in tuple(request.get("quality_paths", ())):
            continue
        if str(request.get("quality_report_path", "")) != expected_report:
            continue
        return job
    raise SystemExit(
        f"runtime jobs did not include quality request for {expected_target}"
    )


def _validate_quality_job(
    job: Mapping[str, Any],
    *,
    expected_status: str,
) -> None:
    if _normalized_quality_job_status(job) != expected_status:
        raise SystemExit(
            "runtime quality job had unexpected status: "
            f"expected={expected_status} job={job}"
        )
    artifacts = job.get("artifacts")
    if not isinstance(artifacts, list) or not any(
        isinstance(artifact, Mapping) and artifact.get("kind") == "quality-report"
        for artifact in artifacts
    ):
        raise SystemExit(f"runtime quality job missing quality-report artifact: {job}")


def _normalized_quality_job_status(job: Mapping[str, Any]) -> str:
    return str(job.get("status", "") or "").strip().lower()


def _stop_quality_runtime(
    *,
    workspace: Path,
    runtime_home: Path,
    stop_timeout: float,
    env: Mapping[str, str],
) -> dict[str, Any]:
    return _run_json(
        [
            _script_path("histdatacom"),
            "runtime",
            "--workspace",
            str(workspace),
            "--runtime-home",
            str(runtime_home),
            "--json",
            "stop",
            "--stop-timeout",
            str(stop_timeout),
        ],
        env=env,
        timeout=stop_timeout + 60.0,
    )


def _validate_quality_runtime_stop(payload: Mapping[str, Any]) -> None:
    if payload.get("state") != "stopped":
        raise SystemExit(f"quality runtime did not stop cleanly: {payload}")
    pids = payload.get("pids")
    if isinstance(pids, Mapping) and pids:
        raise SystemExit(f"quality runtime stop left running processes: {payload}")


def _quality_smoke_case_result(
    *,
    completed: subprocess.CompletedProcess[str],
    report_path: Path,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    summary = payload.get("summary")
    return {
        "returncode": completed.returncode,
        "report": str(report_path),
        "status": (
            str(summary.get("status", "")) if isinstance(summary, Mapping) else ""
        ),
    }


def main() -> None:
    """Run install-time runtime smoke checks."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--wheel-dir",
        type=Path,
        help="install the only histdatacom wheel from this directory first",
    )
    parser.add_argument(
        "--wheel",
        type=Path,
        help="install this exact histdatacom wheel first",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help="state directory used for offline runtime CLI checks",
    )
    parser.add_argument(
        "--expect-temporal-extra",
        action="store_true",
        help="require the temporalio optional dependency to be installed",
    )
    parser.add_argument(
        "--skip-cli",
        action="store_true",
        help="skip console command execution and validate import metadata only",
    )
    parser.add_argument(
        "--require-bundled-current-platform",
        action="store_true",
        help="require the installed wheel to bundle this platform executable",
    )
    parser.add_argument(
        "--require-external-runtime-provisioning",
        action="store_true",
        help=(
            "require the installed wheel to use the pinned external Temporal "
            "runtime resolver instead of a bundled or explicit executable"
        ),
    )
    parser.add_argument(
        "--check-executable-version",
        action="store_true",
        help="run the packaged Temporal executable with --version",
    )
    parser.add_argument(
        "--start-runtime",
        action="store_true",
        help="start the runtime without --executable and then stop it",
    )
    parser.add_argument(
        "--windows-runtime-diagnostic",
        action="store_true",
        help=(
            "collect layered Windows startup diagnostics before the blocking "
            "runtime smoke"
        ),
    )
    parser.add_argument(
        "--live-runtime-smoke",
        action="store_true",
        help=(
            "external HistData.com operator-gated smoke that starts Temporal "
            "workers, submits a URL-validation job, and validates "
            "status/artifacts"
        ),
    )
    parser.add_argument(
        "--hermetic-runtime-smoke",
        action="store_true",
        help=(
            "deterministic installed-wheel smoke that starts Temporal workers, "
            "submits a local-only dataset-planning job, and validates "
            "status/artifacts"
        ),
    )
    parser.add_argument(
        "--default-routing-runtime-smoke",
        action="store_true",
        help=(
            "deterministic installed-wheel smoke that starts Temporal with "
            "non-default worker routing, submits without an explicit worker "
            "config, and validates default client resolver routing"
        ),
    )
    parser.add_argument(
        "--quality-runtime-smoke",
        action="store_true",
        help=(
            "deterministic installed-wheel smoke that runs clean and dirty "
            "histdatacom --quality commands through the local runtime"
        ),
    )
    parser.add_argument(
        "--temporal-executable",
        type=Path,
        help=(
            "Temporal executable for live runtime smokes; defaults to "
            "HISTDATACOM_TEMPORAL_EXECUTABLE or the packaged executable"
        ),
    )
    parser.add_argument(
        "--live-workspace",
        type=Path,
        help="workspace path used for live runtime smoke scoping",
    )
    parser.add_argument(
        "--live-runtime-home",
        type=Path,
        help="runtime home used for live smoke state/logs/SQLite",
    )
    parser.add_argument(
        "--live-data-dir",
        type=Path,
        help="HistData data directory used by the live runtime smoke job",
    )
    parser.add_argument(
        "--live-startup-timeout",
        type=float,
        default=30.0,
        help="seconds to wait for the live Temporal frontend to start",
    )
    parser.add_argument(
        "--live-completion-timeout",
        type=float,
        default=180.0,
        help="seconds to wait for the live smoke job to complete",
    )
    parser.add_argument(
        "--live-stop-timeout",
        type=float,
        default=30.0,
        help="seconds to wait for live runtime processes to stop",
    )
    args = parser.parse_args()
    if args.wheel is not None and args.wheel_dir is not None:
        parser.error("--wheel and --wheel-dir are mutually exclusive")

    wheel_name = ""
    if args.wheel_dir is not None or args.wheel is not None:
        wheel_name = install_wheel(
            wheel_dir=args.wheel_dir,
            wheel_path=args.wheel,
            install_temporal_extra=args.expect_temporal_extra,
        ).name

    with tempfile.TemporaryDirectory() as temporary_dir:
        state_dir = args.state_dir or Path(temporary_dir) / "runtime-state"
        report = {
            "wheel": wheel_name,
            "package": check_package_metadata(
                expect_temporal_extra=args.expect_temporal_extra
            ),
            "runtime": check_runtime_resources(
                require_bundled_current_platform=(
                    args.require_bundled_current_platform
                ),
                require_external_runtime_provisioning=(
                    args.require_external_runtime_provisioning
                ),
                check_executable_version=args.check_executable_version,
                temporal_executable=args.temporal_executable,
            ),
            "cli": None,
            "windows_runtime_diagnostic": None,
            "hermetic_runtime": None,
            "default_routing_runtime": None,
            "quality_runtime": None,
            "live_runtime": None,
        }
        if args.windows_runtime_diagnostic:
            report["windows_runtime_diagnostic"] = check_windows_runtime_diagnostic(
                state_dir,
                startup_timeout=args.live_startup_timeout,
                stop_timeout=args.live_stop_timeout,
            )
        if not args.skip_cli:
            report["cli"] = check_cli_smoke(
                state_dir,
                require_bundled_current_platform=(
                    args.require_bundled_current_platform
                ),
                start_runtime=args.start_runtime,
                startup_timeout=args.live_startup_timeout,
                stop_timeout=args.live_stop_timeout,
            )
        if args.hermetic_runtime_smoke:
            live_workspace = args.live_workspace or Path(temporary_dir) / (
                "live-workspace"
            )
            live_runtime_home = (
                args.live_runtime_home or Path(temporary_dir) / "live-runtime"
            )
            live_data_dir = args.live_data_dir or Path(temporary_dir) / ("live-data")
            report["hermetic_runtime"] = check_hermetic_runtime_smoke(
                workspace=live_workspace,
                runtime_home=live_runtime_home,
                data_directory=live_data_dir,
                temporal_executable=args.temporal_executable,
                startup_timeout=args.live_startup_timeout,
                completion_timeout=args.live_completion_timeout,
                stop_timeout=args.live_stop_timeout,
            )
        if args.default_routing_runtime_smoke:
            live_workspace = args.live_workspace or Path(temporary_dir) / (
                "live-workspace"
            )
            live_runtime_home = (
                args.live_runtime_home or Path(temporary_dir) / "live-runtime"
            )
            live_data_dir = args.live_data_dir or Path(temporary_dir) / ("live-data")
            report["default_routing_runtime"] = check_default_routing_runtime_smoke(
                workspace=live_workspace,
                runtime_home=live_runtime_home,
                data_directory=live_data_dir,
                temporal_executable=args.temporal_executable,
                startup_timeout=args.live_startup_timeout,
                completion_timeout=args.live_completion_timeout,
                stop_timeout=args.live_stop_timeout,
            )
        if args.quality_runtime_smoke:
            live_workspace = args.live_workspace or Path(temporary_dir) / (
                "live-workspace"
            )
            live_runtime_home = (
                args.live_runtime_home or Path(temporary_dir) / "live-runtime"
            )
            live_data_dir = args.live_data_dir or Path(temporary_dir) / ("live-data")
            report["quality_runtime"] = check_quality_runtime_smoke(
                workspace=live_workspace,
                runtime_home=live_runtime_home,
                data_directory=live_data_dir,
                temporal_executable=args.temporal_executable,
                startup_timeout=args.live_startup_timeout,
                stop_timeout=args.live_stop_timeout,
            )
        if args.live_runtime_smoke:
            live_workspace = args.live_workspace or Path(temporary_dir) / (
                "live-workspace"
            )
            live_runtime_home = (
                args.live_runtime_home or Path(temporary_dir) / "live-runtime"
            )
            live_data_dir = args.live_data_dir or Path(temporary_dir) / ("live-data")
            report["live_runtime"] = check_live_runtime_smoke(
                workspace=live_workspace,
                runtime_home=live_runtime_home,
                data_directory=live_data_dir,
                temporal_executable=args.temporal_executable,
                startup_timeout=args.live_startup_timeout,
                completion_timeout=args.live_completion_timeout,
                stop_timeout=args.live_stop_timeout,
            )

    print(json.dumps(report, indent=2, sort_keys=True))  # noqa:T201


if __name__ == "__main__":
    main()

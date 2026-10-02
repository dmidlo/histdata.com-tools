"""Test-only bounded worker launcher; direct exit never proves native cleanup.

Used only by the authored reconnect regression. Failure must stop physical
qualification (-x); preserve its directory and inspect native cleanup before
any later case. No fallback or retry turns a timeout/interruption into success.
"""

from __future__ import annotations

import math
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ConformanceWorkerCleanup:
    """Diagnostic state attached to the original exception, not a receipt."""

    terminate_requested: bool
    graceful_direct_exit: bool
    hard_escalation: bool
    direct_reaped: bool
    native_cleanup: str
    failures: tuple[str, ...]
    stdout: bytes | None
    stderr: bytes | None


def _failed_worker_cleanup(
    process,
    original: BaseException,
    *,
    grace_timeout: float,
    kill_timeout: float,
) -> ConformanceWorkerCleanup:
    failures = []
    stdout = getattr(original, "output", None)
    stderr = getattr(original, "stderr", None)
    terminated = graceful = escalated = reaped = False

    def communicate(timeout):
        nonlocal stdout, stderr, reaped
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except BaseException as error:
            if getattr(error, "output", None) is not None:
                stdout = error.output
            if getattr(error, "stderr", None) is not None:
                stderr = error.stderr
            raise
        if process.returncode is None:
            raise RuntimeError("communicate returned without a direct status")
        reaped = True

    try:
        # _worker translates SIGTERM into KeyboardInterrupt so the owned
        # native supervisor can unwind/reap its separate process group.
        process.terminate()
        terminated = True
    except BaseException as error:  # noqa: BLE001 - Preserve original failure.
        failures.append("terminate:" + type(error).__name__)
    try:
        communicate(grace_timeout)
        graceful = terminated and not failures
    except BaseException as error:  # noqa: BLE001 - Preserve original failure.
        failures.append("grace_communicate:" + type(error).__name__)
        escalated = True
        try:
            process.kill()
        except BaseException as error:  # noqa: BLE001 - Keep cleanup bounded.
            failures.append("kill:" + type(error).__name__)
        try:
            communicate(kill_timeout)
        except BaseException as error:  # noqa: BLE001 - Keep original failure.
            failures.append("kill_communicate:" + type(error).__name__)
    finally:
        # No Popen context manager: __exit__ can wait without a bound. Failed
        # cleanup closes our pipes without pretending a child was reaped.
        for name in ("stdout", "stderr"):
            stream = getattr(process, name, None)
            if stream is not None:
                try:
                    stream.close()
                except BaseException as error:  # noqa: BLE001
                    failures.append(name + "_close:" + type(error).__name__)
    return ConformanceWorkerCleanup(
        terminate_requested=terminated,
        graceful_direct_exit=graceful,
        hard_escalation=escalated,
        direct_reaped=reaped,
        native_cleanup="unknown",
        failures=tuple(failures),
        stdout=stdout,
        stderr=stderr,
    )


def run_conformance_worker(
    command: list[str],
    *,
    cwd: Path,
    timeout: float,
    grace_timeout: float = 15,
    kill_timeout: float = 5,
) -> subprocess.CompletedProcess:
    """Run once; preserve original failure and mark native cleanup unknown.

    Only a successful returned native manifest can establish worker_reaped.
    Even graceful direct-worker exit during cleanup remains unverified here.
    """
    for value, ceiling in (
        (timeout, 3600),
        (grace_timeout, 30),
        (kill_timeout, 10),
    ):
        if not math.isfinite(value) or not 0 < value <= ceiling:
            raise ValueError("finite bounded subprocess deadlines required")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        if process.returncode is None:
            raise RuntimeError("communicate returned without a direct status")
    except BaseException as original:
        cleanup = _failed_worker_cleanup(
            process,
            original,
            grace_timeout=grace_timeout,
            kill_timeout=kill_timeout,
        )
        original.conformance_cleanup = cleanup
        message = (
            "Conformance worker failed or was interrupted; "
            "native cleanup UNKNOWN. Stop qualification and inspect "
            "retained artifacts. "
            f"direct_reaped={cleanup.direct_reaped}; "
            f"hard_escalation={cleanup.hard_escalation}."
        )
        # Closed diagnostic text only; never replace the original error or
        # KeyboardInterrupt if diagnostic output itself fails.
        try:
            sys.stderr.write(message + "\n")
            add_note = getattr(original, "add_note", None)
            if add_note is not None:
                add_note(message)
        except BaseException:  # noqa: BLE001 - Do not mask interruption.
            original.conformance_cleanup_diagnostic_failed = True
        raise
    return subprocess.CompletedProcess(
        command, process.returncode, stdout, stderr
    )

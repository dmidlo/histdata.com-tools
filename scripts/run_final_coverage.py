#!/usr/bin/env python3
"""Run the authoritative full coverage gate once per repository content state."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECEIPT_SCHEMA_VERSION = "histdatacom.final-coverage-receipt.v1"
RECEIPT_PATH = Path(".histdatacom") / "coverage" / "final-coverage.json"
LOCK_PATH = Path(".histdatacom") / "coverage" / "final-coverage.lock"
DEFAULT_LOCK_TIMEOUT_SECONDS = 30.0
STALE_LOCK_SECONDS = 6 * 60 * 60
CommandRunner = Callable[
    [Sequence[str], Path],
    subprocess.CompletedProcess[str],
]
PathProvider = Callable[[Path], Sequence[str]]


class FinalCoverageError(RuntimeError):
    """A final-coverage gate failure with a stable machine-readable code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def repository_content_paths(repo_root: Path) -> tuple[str, ...]:
    """Return tracked and unignored untracked repository paths."""
    result = subprocess.run(  # noqa:S603
        (
            "git",
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ),
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise FinalCoverageError(
            "repository-paths-unavailable",
            "Unable to enumerate repository content for final coverage.",
        )
    values = result.stdout.decode("utf-8", errors="surrogateescape").split("\0")
    return tuple(sorted(value for value in values if value))


def repository_content_fingerprint(
    repo_root: Path,
    paths: Iterable[str],
) -> str:
    """Hash repository-relative paths, file kinds, modes, and bytes."""
    digest = hashlib.sha256()
    for value in sorted(set(str(path) for path in paths if str(path))):
        relative = Path(value)
        if relative.is_absolute() or ".." in relative.parts:
            raise FinalCoverageError(
                "unsafe-repository-path",
                "Repository fingerprint input contains an unsafe path.",
            )
        path = repo_root / relative
        digest.update(b"path\0")
        digest.update(value.encode("utf-8", errors="surrogateescape"))
        digest.update(b"\0")
        try:
            stat = path.lstat()
        except FileNotFoundError:
            digest.update(b"missing\0")
            continue
        digest.update(f"mode:{stat.st_mode & 0o111:o}\0".encode())
        if path.is_symlink():
            digest.update(b"symlink\0")
            digest.update(os.readlink(path).encode("utf-8", errors="surrogateescape"))
            digest.update(b"\0")
            continue
        if not path.is_file():
            digest.update(b"non-file\0")
            continue
        digest.update(b"file\0")
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def runtime_versions() -> dict[str, str]:
    """Return the tool versions that make coverage evidence comparable."""
    return {
        "python": ".".join(str(part) for part in sys.version_info[:3]),
        "pytest": importlib.metadata.version("pytest"),
        "coverage": importlib.metadata.version("coverage"),
    }


def coverage_evidence_key(
    content_fingerprint: str,
    versions: dict[str, str],
) -> str:
    """Return the stable receipt key for content plus runtime versions."""
    payload = json.dumps(
        {
            "content_fingerprint": content_fingerprint,
            "versions": versions,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_final_coverage(
    repo_root: Path = PROJECT_ROOT,
    *,
    force: bool = False,
    runner: CommandRunner | None = None,
    path_provider: PathProvider = repository_content_paths,
    versions: dict[str, str] | None = None,
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Execute or reuse final coverage for the current content fingerprint."""
    command_runner = runner or _run_command
    resolved_versions = dict(versions if versions is not None else runtime_versions())
    receipt_path = repo_root / RECEIPT_PATH
    paths = tuple(path_provider(repo_root))
    content_fingerprint = repository_content_fingerprint(repo_root, paths)
    evidence_key = coverage_evidence_key(content_fingerprint, resolved_versions)
    receipt = _read_receipt(receipt_path)
    if not force and _receipt_matches(receipt, evidence_key):
        return _result_from_receipt(receipt, decision="reused")

    with _coverage_lock(
        repo_root,
        timeout_seconds=lock_timeout_seconds,
    ):
        paths = tuple(path_provider(repo_root))
        content_fingerprint = repository_content_fingerprint(repo_root, paths)
        evidence_key = coverage_evidence_key(
            content_fingerprint,
            resolved_versions,
        )
        receipt = _read_receipt(receipt_path)
        if not force and _receipt_matches(receipt, evidence_key):
            return _result_from_receipt(receipt, decision="reused")

        receipt_path.unlink(missing_ok=True)
        commands = _coverage_commands()
        command_results: list[dict[str, Any]] = []
        try:
            for command in commands:
                result = command_runner(command, repo_root)
                command_results.append(
                    {
                        "command": list(command[2:]),
                        "returncode": int(result.returncode),
                    }
                )
                if result.returncode != 0:
                    raise FinalCoverageError(
                        "coverage-command-failed",
                        "Final coverage failed while running "
                        + " ".join(command[2:])
                        + ".",
                    )

            after_paths = tuple(path_provider(repo_root))
            after_fingerprint = repository_content_fingerprint(
                repo_root,
                after_paths,
            )
            if after_fingerprint != content_fingerprint:
                raise FinalCoverageError(
                    "repository-content-changed",
                    "Repository content changed while final coverage was running.",
                )

            receipt = {
                "schema_version": RECEIPT_SCHEMA_VERSION,
                "state": "pass",
                "evidence_key": evidence_key,
                "content_fingerprint": content_fingerprint,
                "versions": resolved_versions,
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                "commands": command_results,
            }
            _write_receipt(receipt_path, receipt)
            return _result_from_receipt(receipt, decision="executed")
        finally:
            _remove_coverage_data(repo_root)


def _coverage_commands() -> tuple[tuple[str, ...], ...]:
    return (
        (sys.executable, "-m", "coverage", "erase"),
        (sys.executable, "-m", "coverage", "run"),
        (sys.executable, "-m", "coverage", "combine"),
        (sys.executable, "-m", "coverage", "report"),
    )


def _run_command(
    command: Sequence[str],
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa:S603
        list(command),
        cwd=cwd,
        check=False,
        text=True,
    )


def _read_receipt(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _receipt_matches(receipt: dict[str, Any], evidence_key: str) -> bool:
    return (
        receipt.get("schema_version") == RECEIPT_SCHEMA_VERSION
        and receipt.get("state") == "pass"
        and receipt.get("evidence_key") == evidence_key
    )


def _result_from_receipt(
    receipt: dict[str, Any],
    *,
    decision: str,
) -> dict[str, Any]:
    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "state": "pass",
        "decision": decision,
        "evidence_key": str(receipt.get("evidence_key", "")),
        "content_fingerprint": str(receipt.get("content_fingerprint", "")),
        "versions": dict(receipt.get("versions") or {}),
        "completed_at_utc": str(receipt.get("completed_at_utc", "")),
    }


def _write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp_path.write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


@contextmanager
def _coverage_lock(
    repo_root: Path,
    *,
    timeout_seconds: float,
) -> Iterable[None]:
    lock_path = repo_root / LOCK_PATH
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while True:
        try:
            descriptor = os.open(  # noqa:PTH123
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            if _lock_is_stale(lock_path):
                lock_path.unlink(missing_ok=True)
                continue
            if time.monotonic() >= deadline:
                raise FinalCoverageError(
                    "coverage-lock-timeout",
                    "Another final coverage process is already running.",
                )
            time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
            continue
        with os.fdopen(descriptor, "w", encoding="utf-8") as sink:
            sink.write(f"pid={os.getpid()}\n")
        break
    try:
        yield
    finally:
        lock_path.unlink(missing_ok=True)


def _lock_is_stale(lock_path: Path) -> bool:
    try:
        age = time.time() - lock_path.stat().st_mtime
    except FileNotFoundError:
        return True
    if age <= STALE_LOCK_SECONDS:
        return False
    try:
        first_line = lock_path.read_text(encoding="utf-8").splitlines()[0]
        owner_pid = int(first_line.removeprefix("pid="))
    except (IndexError, OSError, ValueError):
        return True
    try:
        os.kill(owner_pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _remove_coverage_data(repo_root: Path) -> None:
    paths = (repo_root / ".coverage", *repo_root.glob(".coverage.*"))
    for path in paths:
        if path.is_file():
            path.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute or reuse the final full-suite coverage gate.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--ensure",
        action="store_true",
        help="reuse matching coverage evidence or execute coverage once",
    )
    mode.add_argument(
        "--force",
        action="store_true",
        help="ignore a matching receipt and execute coverage again",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print a machine-readable result",
    )
    parser.add_argument(
        "--lock-timeout",
        type=float,
        default=DEFAULT_LOCK_TIMEOUT_SECONDS,
        help="seconds to wait for another final coverage process",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the final coverage command-line interface."""
    args = _parser().parse_args(argv)
    try:
        result = ensure_final_coverage(
            PROJECT_ROOT,
            force=bool(args.force),
            lock_timeout_seconds=float(args.lock_timeout),
        )
    except FinalCoverageError as exc:
        payload = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "state": "fail",
            "code": exc.code,
            "message": str(exc),
        }
        if args.json:
            print(json.dumps(payload, sort_keys=True))  # noqa:T201
        else:
            print(f"Final coverage failed [{exc.code}]: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, sort_keys=True))  # noqa:T201
    else:
        print(  # noqa:T201
            "Final coverage "
            f"{result['decision']}: {str(result['evidence_key'])[:12]}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

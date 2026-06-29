#!/usr/bin/env python3
"""Generate publish-safe issue closure readiness reports."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import shlex
import subprocess
import sys
from time import perf_counter
from typing import Any, TextIO

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from histdatacom.publication_safety import (  # noqa: E402
    publish_safe_json_value,
    publish_safe_path,
)
from histdatacom.source_cleanup import (  # noqa: E402
    source_artifact_cleanliness_payload,
)

SCHEMA_VERSION = "histdatacom.closure-readiness.v1"
SUMMARY_SCHEMA_VERSION = "histdatacom.closure-report-summary.v1"
COMMIT_READINESS_SCHEMA_VERSION = "histdatacom.commit-readiness.v1"
ISSUE_WORKFLOW_SCHEMA_VERSION = "histdatacom.issue-workflow-execution.v1"
ISSUE_WORKFLOW_SUMMARY_SCHEMA_VERSION = "histdatacom.issue-workflow-summary.v1"
ACCEPTANCE_COVERAGE_SCHEMA_VERSION = "histdatacom.acceptance-coverage.v1"
DEFAULT_REPORT_DIR = Path(".histdatacom") / "closure-readiness"
DEFAULT_REQUIRED_BRANCH = "dev"
DEFAULT_EXPECTED_UPSTREAM = "origin/dev"
DEFAULT_TAIL_LINE_LIMIT = 12
DEFAULT_TAIL_CHAR_LIMIT = 4_000
ACCEPTANCE_STATUSES = {
    "verified",
    "manual",
    "not-applicable",
    "missing",
}
ACCEPTANCE_MISSING_BLOCKER = "acceptance-criteria-missing"
CommandRunner = Callable[
    [Sequence[str], Path],
    subprocess.CompletedProcess[str],
]


@dataclass(frozen=True, slots=True)
class GateSpec:
    """One closure-readiness command gate."""

    name: str
    command: tuple[str, ...]
    display: str


@dataclass(frozen=True, slots=True)
class ProcessObservation:
    """A bounded process match without command-line details."""

    pid: int
    category: str
    command: str


class WorkflowExecutionLogger:
    """Run commands while storing bounded evidence and full local logs."""

    def __init__(
        self,
        *,
        repo_root: Path,
        log_dir: Path,
        runner: CommandRunner,
    ) -> None:
        self.repo_root = repo_root
        self.log_dir = log_dir
        self.runner = runner
        self.records: list[dict[str, Any]] = []
        self._counter = 0

    def __call__(
        self,
        command: Sequence[str],
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        """Run one inferred workflow command step."""
        return self.run(command, cwd, name=_workflow_command_name(command))

    def run(
        self,
        command: Sequence[str],
        cwd: Path,
        *,
        name: str,
    ) -> subprocess.CompletedProcess[str]:
        """Run one named command and record bounded plus full output."""
        result = self.runner(command, cwd)
        self._counter += 1
        payload = _workflow_command_payload(
            command,
            result,
            name=name,
            log_path=self._write_command_log(
                command,
                result,
                name=name,
                index=self._counter,
                cwd=cwd,
            ),
            repo_root=self.repo_root,
        )
        self.records.append(payload)
        return result

    def _write_command_log(
        self,
        command: Sequence[str],
        result: subprocess.CompletedProcess[str],
        *,
        name: str,
        index: int,
        cwd: Path,
    ) -> Path:
        filename = f"{index:03d}-{_slug(name)}.log"
        path = self.log_dir / filename
        body = [
            f"command: {_shell_command(command)}",
            f"cwd: {publish_safe_path(str(cwd))}",
            f"returncode: {result.returncode}",
            "",
            "stdout:",
            result.stdout or "",
            "",
            "stderr:",
            result.stderr or "",
        ]
        _write_text(path, "\n".join(body))
        return path


class WorkflowProgressReporter:
    """Record and optionally stream executable workflow phase progress."""

    def __init__(
        self,
        *,
        stream: TextIO | None,
    ) -> None:
        self.stream = stream
        self._started_at = perf_counter()
        self._last_completed_phase = ""
        self._phase_started: dict[str, float] = {}
        self._phases: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []

    def start(
        self,
        phase: str,
        *,
        message: str = "",
        command_label: str = "",
        log_path: str = "",
        report_path: str = "",
    ) -> None:
        """Record a phase start event."""
        now = perf_counter()
        self._phase_started[phase] = now
        self._phases[phase] = {
            "phase": phase,
            "status": "started",
            "started_elapsed_seconds": self._elapsed(now),
            "finished_elapsed_seconds": None,
            "duration_seconds": None,
            "message": message,
            "command_label": command_label,
            "log_path": log_path,
            "report_path": report_path,
        }
        self._emit(
            phase,
            "started",
            message=message,
            command_label=command_label,
            log_path=log_path,
            report_path=report_path,
            now=now,
        )

    def finish(
        self,
        phase: str,
        *,
        status: str = "completed",
        message: str = "",
        command_label: str = "",
        log_path: str = "",
        report_path: str = "",
    ) -> None:
        """Record a terminal phase event."""
        now = perf_counter()
        started_at = self._phase_started.get(phase)
        duration = (
            round(now - started_at, 3) if started_at is not None else None
        )
        if status == "completed":
            self._last_completed_phase = phase
        phase_payload = self._phases.get(
            phase,
            {
                "phase": phase,
                "started_elapsed_seconds": None,
            },
        )
        phase_payload.update(
            {
                "status": status,
                "finished_elapsed_seconds": self._elapsed(now),
                "duration_seconds": duration,
                "message": message,
                "command_label": command_label,
                "log_path": log_path,
                "report_path": report_path,
            }
        )
        self._phases[phase] = phase_payload
        self._emit(
            phase,
            status,
            message=message,
            command_label=command_label,
            log_path=log_path,
            report_path=report_path,
            now=now,
            duration_seconds=duration,
        )

    def skip(self, phase: str, *, message: str = "") -> None:
        """Record a skipped phase without separate start and finish events."""
        self.finish(phase, status="skipped", message=message)

    def payload(self) -> dict[str, Any]:
        """Return publish-safe progress evidence for the workflow report."""
        statuses = {str(event.get("status", "")) for event in self.events}
        if "failed" in statuses:
            state = "failed"
        elif "blocked" in statuses:
            state = "blocked"
        elif self._phases:
            state = "completed"
        else:
            state = "not-started"
        payload = {
            "state": state,
            "stream": "stderr-line" if self.stream is not None else "quiet",
            "event_count": len(self.events),
            "phase_count": len(self._phases),
            "elapsed_seconds": self._elapsed(perf_counter()),
            "last_completed_phase": self._last_completed_phase,
            "phases": list(self._phases.values()),
            "events": self.events,
        }
        safe = publish_safe_json_value(payload)
        if not isinstance(safe, dict):
            raise TypeError("workflow progress payload must be a JSON object")
        return dict(safe)

    def _emit(
        self,
        phase: str,
        status: str,
        *,
        message: str,
        command_label: str,
        log_path: str,
        report_path: str,
        now: float,
        duration_seconds: float | None = None,
    ) -> None:
        elapsed = self._elapsed(now)
        event: dict[str, Any] = {
            "phase": phase,
            "status": status,
            "current_phase": phase,
            "last_completed_phase": self._last_completed_phase,
            "elapsed_seconds": elapsed,
            "message": message,
        }
        if duration_seconds is not None:
            event["duration_seconds"] = duration_seconds
        if command_label:
            event["command_label"] = command_label
        if log_path:
            event["log_path"] = str(publish_safe_path(log_path))
        if report_path:
            event["report_path"] = str(publish_safe_path(report_path))
        self.events.append(event)
        if self.stream is None:
            return
        parts = [
            "issue-workflow progress:",
            f"phase={phase}",
            f"status={status}",
            f"current={phase}",
            f"last={self._last_completed_phase or '-'}",
            f"elapsed={elapsed:.3f}s",
        ]
        if duration_seconds is not None:
            parts.append(f"duration={duration_seconds:.3f}s")
        if command_label:
            parts.append(f"command={command_label}")
        if log_path:
            parts.append(f"log={publish_safe_path(log_path)}")
        if report_path:
            parts.append(f"report={publish_safe_path(report_path)}")
        if message:
            parts.append(f"message={message}")
        print(" ".join(parts), file=self.stream, flush=True)  # noqa: T201

    def _elapsed(self, now: float) -> float:
        return round(now - self._started_at, 3)


GATE_SPECS = (
    GateSpec(
        "readme-help-sync",
        (sys.executable, "scripts/sync_readme_cli_help.py", "--check"),
        "python scripts/sync_readme_cli_help.py --check",
    ),
    GateSpec("git-diff-check", ("git", "diff", "--check"), "git diff --check"),
    GateSpec(
        "main-help-smoke",
        (sys.executable, "-m", "histdatacom", "--help"),
        "python -m histdatacom --help",
    ),
    GateSpec("pytest", (sys.executable, "-m", "pytest"), "python -m pytest"),
    GateSpec(
        "pre-commit",
        (sys.executable, "-m", "pre_commit", "run", "--all-files"),
        "python -m pre_commit run --all-files",
    ),
)
RELEASE_PREFLIGHT_GATE = GateSpec(
    "testpypi-local-preflight",
    ("bash", "pypi.sh", "testpypi_preflight"),
    "bash pypi.sh testpypi_preflight",
)
PROCESS_CATEGORY_ORDER = (
    "pytest",
    "pre-commit",
    "temporal-worker",
    "temporal-runtime",
    "histdatacom",
    "ruff",
    "mypy",
)


def build_readiness_report(
    *,
    repo_root: Path = PROJECT_ROOT,
    issue: int | None = None,
    run_gates: bool = False,
    release_preflight: bool = False,
    artifact_roots: Sequence[Path] | None = None,
    process_rows: Sequence[str] | None = None,
    runner: CommandRunner | None = None,
    now: datetime | None = None,
    acceptance_statuses: Mapping[str, str] | None = None,
    acceptance_files: Mapping[str, Sequence[str]] | None = None,
    acceptance_tests: Mapping[str, Sequence[str]] | None = None,
    acceptance_reports: Mapping[str, Sequence[str]] | None = None,
    acceptance_notes: Mapping[str, Sequence[str]] | None = None,
    acceptance_missing_ok: bool = False,
    acceptance_override_reason: str = "",
) -> dict[str, Any]:
    """Return a publish-safe closure-readiness report."""
    command_runner = runner or _run_command
    root = repo_root.expanduser().resolve(strict=False)
    generated_at = now or datetime.now(timezone.utc)
    git_state_before_gates = collect_git_state(root, runner=command_runner)
    issue_state = collect_issue_state(
        root,
        issue,
        runner=command_runner,
        include_body=True,
    )
    acceptance_coverage = build_acceptance_coverage(
        issue_state,
        statuses=acceptance_statuses,
        files=acceptance_files,
        tests=acceptance_tests,
        reports=acceptance_reports,
        notes=acceptance_notes,
        missing_ok=acceptance_missing_ok,
        override_reason=acceptance_override_reason,
    )
    processes_before_gates = collect_process_summary(
        root,
        rows=process_rows,
        runner=command_runner,
    )
    source_artifacts_before_gates = collect_artifact_summary(
        artifact_roots if artifact_roots is not None else (root / "data",),
    )
    gates = collect_gate_summary(
        root,
        run_gates=run_gates,
        runner=command_runner,
    )
    release = collect_release_preflight(
        root,
        run=release_preflight,
        runner=command_runner,
    )
    git_state = collect_git_state(root, runner=command_runner)
    process_summary = collect_process_summary(
        root,
        rows=process_rows,
        runner=command_runner,
    )
    artifact_summary = collect_artifact_summary(
        artifact_roots if artifact_roots is not None else (root / "data",),
    )
    readiness = determine_readiness(
        git_state=git_state,
        issue_state=issue_state,
        process_summary=process_summary,
        artifact_summary=artifact_summary,
        gates=gates,
        release_preflight=release,
    )
    precheck = determine_precheck(
        git_state=git_state,
        issue_state=issue_state,
        process_summary=process_summary,
        artifact_summary=artifact_summary,
        issue=issue,
    )
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "operation": "closure-readiness",
        "generated_at_utc": generated_at.astimezone(timezone.utc).isoformat(),
        "repo": {
            "root": str(publish_safe_path(str(root))),
            **git_state,
        },
        "issue": _issue_report_payload(issue_state),
        "acceptance_coverage": acceptance_coverage,
        "processes": process_summary,
        "processes_before_gates": processes_before_gates,
        "source_artifacts": artifact_summary,
        "source_artifacts_before_gates": source_artifacts_before_gates,
        "gates": gates,
        "release_preflight": release,
        "precheck": precheck,
        "readiness": readiness,
    }
    if git_state_before_gates != git_state:
        report["repo_before_gates"] = git_state_before_gates
    report["close_comment"] = render_close_comment(report)
    safe = publish_safe_json_value(report)
    if not isinstance(safe, dict):
        raise TypeError("closure readiness report must be a JSON object")
    return dict(safe)


def collect_git_state(
    repo_root: Path,
    *,
    runner: CommandRunner,
) -> dict[str, Any]:
    """Return current branch, upstream, dirty, and alignment state."""
    branch = _git_stdout(
        repo_root, ("rev-parse", "--abbrev-ref", "HEAD"), runner
    )
    head = _git_stdout(repo_root, ("rev-parse", "HEAD"), runner)
    upstream_result = _run_git(
        repo_root,
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"),
        runner,
    )
    upstream = (
        upstream_result.stdout.strip()
        if upstream_result.returncode == 0
        else ""
    )
    status_result = _run_git(
        repo_root,
        ("status", "--porcelain=v1", "--untracked-files=all"),
        runner,
    )
    status_lines = tuple(
        line for line in status_result.stdout.splitlines() if line.strip()
    )
    changed_paths = _status_paths(status_lines)
    upstream_counts = _upstream_counts(repo_root, bool(upstream), runner)
    dirty = bool(status_lines)
    aligned = (
        bool(upstream)
        and upstream_counts["ahead"] == 0
        and upstream_counts["behind"] == 0
    )
    return {
        "branch": branch or "unknown",
        "head": head,
        "head_short": head[:7] if head else "",
        "upstream": upstream,
        "upstream_state": "aligned" if aligned else "not-aligned",
        "ahead": upstream_counts["ahead"],
        "behind": upstream_counts["behind"],
        "dirty": dirty,
        "tracked_dirty_count": sum(
            1 for line in status_lines if not line.startswith("??")
        ),
        "untracked_count": sum(
            1 for line in status_lines if line.startswith("??")
        ),
        "changed_paths": changed_paths,
        "changed_path_count": len(changed_paths),
    }


def build_commit_readiness_report(
    *,
    repo_root: Path = PROJECT_ROOT,
    issue: int | None = None,
    mode: str = "commit",
    commit_message: str = "",
    commit_message_source: str = "argument",
    expected_paths: Sequence[Path] | None = None,
    required_branch: str = DEFAULT_REQUIRED_BRANCH,
    expected_upstream: str = DEFAULT_EXPECTED_UPSTREAM,
    runner: CommandRunner | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return publish-safe commit or push readiness without mutating git."""
    command_runner = runner or _run_command
    root = repo_root.expanduser().resolve(strict=False)
    generated_at = now or datetime.now(timezone.utc)
    git_state = collect_git_state(root, runner=command_runner)
    issue_state = collect_issue_state(root, issue, runner=command_runner)
    changes = collect_change_summary(root, runner=command_runner)
    scope = _commit_scope_payload(
        changes,
        repo_root=root,
        expected_paths=expected_paths or (),
    )
    commit_validation = _commit_message_payload(
        root,
        commit_message=commit_message,
        source=commit_message_source,
        mode=mode,
        runner=command_runner,
    )
    readiness = determine_commit_readiness(
        mode=mode,
        git_state=git_state,
        issue_state=issue_state,
        changes=changes,
        scope=scope,
        commit_message=commit_validation,
        required_branch=required_branch,
        expected_upstream=expected_upstream,
    )
    report: dict[str, Any] = {
        "schema_version": COMMIT_READINESS_SCHEMA_VERSION,
        "operation": f"{mode}-readiness",
        "generated_at_utc": generated_at.astimezone(timezone.utc).isoformat(),
        "mode": mode,
        "required_branch": required_branch,
        "expected_upstream": expected_upstream,
        "repo": {
            "root": str(publish_safe_path(str(root))),
            **git_state,
        },
        "issue": issue_state,
        "changes": changes,
        "scope": scope,
        "commit_message": commit_validation,
        "readiness": readiness,
    }
    report["command_plan"] = _commit_command_plan(report)
    safe = publish_safe_json_value(report)
    if not isinstance(safe, dict):
        raise TypeError("commit readiness report must be a JSON object")
    return dict(safe)


def collect_change_summary(
    repo_root: Path,
    *,
    runner: CommandRunner,
) -> dict[str, Any]:
    """Return staged, unstaged, and untracked paths from porcelain status."""
    result = _run_git(
        repo_root,
        ("status", "--porcelain=v1", "--untracked-files=all"),
        runner,
    )
    if result.returncode != 0:
        return {
            "state": "unavailable",
            "reason": _tail_text(result.stderr or result.stdout),
            "entries": [],
            "staged_paths": [],
            "unstaged_paths": [],
            "untracked_paths": [],
            "changed_paths": [],
            "staged_count": 0,
            "unstaged_count": 0,
            "untracked_count": 0,
            "changed_path_count": 0,
        }
    entries = [
        entry
        for entry in (
            _status_entry(line) for line in result.stdout.splitlines()
        )
        if entry
    ]
    staged = _unique_sorted(
        str(entry["path"]) for entry in entries if entry["staged"]
    )
    unstaged = _unique_sorted(
        str(entry["path"]) for entry in entries if entry["unstaged"]
    )
    untracked = _unique_sorted(
        str(entry["path"]) for entry in entries if entry["untracked"]
    )
    changed = _unique_sorted(str(entry["path"]) for entry in entries)
    return {
        "state": "dirty" if entries else "clean",
        "entries": entries,
        "staged_paths": list(staged),
        "unstaged_paths": list(unstaged),
        "untracked_paths": list(untracked),
        "changed_paths": list(changed),
        "staged_count": len(staged),
        "unstaged_count": len(unstaged),
        "untracked_count": len(untracked),
        "changed_path_count": len(changed),
    }


def collect_issue_state(
    repo_root: Path,
    issue: int | None,
    *,
    runner: CommandRunner,
    include_body: bool = False,
) -> dict[str, Any]:
    """Return linked GitHub issue metadata when requested."""
    if issue is None:
        return {
            "requested": False,
            "state": "not-requested",
            "reason": "no issue number supplied",
        }
    json_fields = "number,state,title,url"
    if include_body:
        json_fields = f"{json_fields},body"
    result = runner(
        (
            "gh",
            "issue",
            "view",
            str(issue),
            "--json",
            json_fields,
        ),
        repo_root,
    )
    if result.returncode != 0:
        return {
            "requested": True,
            "number": issue,
            "state": "unavailable",
            "reason": _tail_text(result.stderr or result.stdout),
        }
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {
            "requested": True,
            "number": issue,
            "state": "unavailable",
            "reason": f"invalid gh issue JSON: {exc}",
        }
    payload_out = {
        "requested": True,
        "number": int(payload.get("number", issue)),
        "state": str(payload.get("state", "unknown")),
        "title": str(payload.get("title", "")),
        "url": str(payload.get("url", "")),
    }
    if include_body:
        payload_out["body"] = str(payload.get("body", "") or "")
    return payload_out


def build_acceptance_coverage(
    issue_state: Mapping[str, Any],
    *,
    statuses: Mapping[str, str] | None = None,
    files: Mapping[str, Sequence[str]] | None = None,
    tests: Mapping[str, Sequence[str]] | None = None,
    reports: Mapping[str, Sequence[str]] | None = None,
    notes: Mapping[str, Sequence[str]] | None = None,
    missing_ok: bool = False,
    override_reason: str = "",
) -> dict[str, Any]:
    """Return publish-safe issue acceptance coverage evidence."""
    if issue_state.get("requested") is False:
        return _empty_acceptance_coverage(
            "not-requested",
            "no issue number supplied",
            missing_ok=missing_ok,
            override_reason=override_reason,
        )
    if issue_state.get("state") == "unavailable":
        return _empty_acceptance_coverage(
            "unavailable",
            str(issue_state.get("reason", "issue body unavailable")),
            missing_ok=missing_ok,
            override_reason=override_reason,
        )
    criteria = parse_acceptance_criteria(str(issue_state.get("body", "") or ""))
    if not criteria:
        return _empty_acceptance_coverage(
            "not-applicable",
            "no acceptance criteria found in issue body",
            missing_ok=missing_ok,
            override_reason=override_reason,
        )

    status_map = dict(statuses or {})
    file_map = _normalize_acceptance_value_map(files)
    test_map = _normalize_acceptance_value_map(tests)
    report_map = _normalize_acceptance_value_map(reports)
    note_map = _normalize_acceptance_value_map(notes)
    items: list[dict[str, Any]] = []
    missing_ids: list[str] = []
    counts = {status: 0 for status in ACCEPTANCE_STATUSES}

    for criterion in criteria:
        criterion_files = _acceptance_values_for(criterion, file_map)
        criterion_tests = _acceptance_values_for(criterion, test_map)
        criterion_reports = _acceptance_values_for(criterion, report_map)
        criterion_notes = _acceptance_values_for(criterion, note_map)
        status = _acceptance_status_for(
            criterion,
            status_map,
            files=criterion_files,
            tests=criterion_tests,
            reports=criterion_reports,
        )
        counts[status] += 1
        if status == "missing":
            missing_ids.append(str(criterion["id"]))
        items.append(
            {
                **criterion,
                "required": status != "not-applicable",
                "status": status,
                "files": _safe_path_list(criterion_files),
                "tests": _safe_text_list(criterion_tests),
                "reports": _safe_path_list(criterion_reports),
                "notes": _safe_text_list(criterion_notes),
            }
        )

    state = "ready"
    reason = "all required acceptance criteria have evidence"
    if missing_ids:
        state = "override" if missing_ok else "blocked"
        reason = (
            "missing acceptance criteria explicitly overridden"
            if missing_ok
            else "required acceptance criteria are missing evidence"
        )
    payload = {
        "schema_version": ACCEPTANCE_COVERAGE_SCHEMA_VERSION,
        "state": state,
        "reason": reason,
        "criteria_count": len(items),
        "required_count": sum(1 for item in items if item["required"]),
        "covered_count": len(items) - len(missing_ids),
        "missing_count": len(missing_ids),
        "counts": {key: counts[key] for key in sorted(counts)},
        "missing_ids": missing_ids,
        "override": {
            "missing_ok": bool(missing_ok),
            "reason": (
                override_reason
                if override_reason
                else (
                    "operator override supplied"
                    if missing_ok
                    else ""
                )
            ),
        },
        "items": items,
    }
    safe = publish_safe_json_value(payload)
    if not isinstance(safe, dict):
        raise TypeError("acceptance coverage report must be a JSON object")
    return dict(safe)


def parse_acceptance_criteria(issue_body: str) -> list[dict[str, Any]]:
    """Parse issue acceptance criteria from a checklist or criteria section."""
    lines = issue_body.splitlines()
    section = _acceptance_section(lines)
    candidates = section if section else lines
    allow_plain_bullets = bool(section)
    parsed: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    for raw_line in candidates:
        bullet = _acceptance_bullet(raw_line, allow_plain=allow_plain_bullets)
        if bullet is not None:
            if current is not None:
                parsed.append(current)
            current = bullet
            continue
        if current is None:
            continue
        if raw_line.startswith((" ", "\t")) and raw_line.strip():
            current["text"] = f"{current['text']} {raw_line.strip()}"
    if current is not None:
        parsed.append(current)

    items: list[dict[str, Any]] = []
    for index, item in enumerate(parsed, start=1):
        text = _normalize_acceptance_text(str(item["text"]))
        if not text:
            continue
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        items.append(
            {
                "id": f"ac-{index:03d}",
                "index": index,
                "slug": _slug(text),
                "text": text,
                "text_sha256": text_hash,
                "text_hash": text_hash[:12],
                "source": item["source"],
                "issue_checked": item["issue_checked"],
            }
        )
    return items


def _empty_acceptance_coverage(
    state: str,
    reason: str,
    *,
    missing_ok: bool,
    override_reason: str,
) -> dict[str, Any]:
    return {
        "schema_version": ACCEPTANCE_COVERAGE_SCHEMA_VERSION,
        "state": state,
        "reason": reason,
        "criteria_count": 0,
        "required_count": 0,
        "covered_count": 0,
        "missing_count": 0,
        "counts": {key: 0 for key in sorted(ACCEPTANCE_STATUSES)},
        "missing_ids": [],
        "override": {
            "missing_ok": bool(missing_ok),
            "reason": override_reason if missing_ok else "",
        },
        "items": [],
    }


def _acceptance_section(lines: Sequence[str]) -> list[str]:
    start_index: int | None = None
    start_level = 0
    for index, line in enumerate(lines):
        heading = re.match(r"^\s{0,3}(#{1,6})\s+(.+?)\s*$", line)
        if not heading:
            continue
        title = heading.group(2).strip().rstrip(":").lower()
        if title in {"acceptance criteria", "acceptance checklist"}:
            start_index = index + 1
            start_level = len(heading.group(1))
            break
    if start_index is None:
        return []
    section: list[str] = []
    for line in lines[start_index:]:
        heading = re.match(r"^\s{0,3}(#{1,6})\s+", line)
        if heading and len(heading.group(1)) <= start_level:
            break
        section.append(line)
    return section


def _acceptance_bullet(
    line: str,
    *,
    allow_plain: bool,
) -> dict[str, Any] | None:
    match = re.match(r"^\s*(?:[-*+]|\d+[.)])\s+(.+?)\s*$", line)
    if not match:
        return None
    body = match.group(1).strip()
    checkbox = re.match(r"^\[(?P<mark>[ xX])\]\s+(.+?)\s*$", body)
    if checkbox:
        return {
            "text": checkbox.group(2).strip(),
            "source": "checklist",
            "issue_checked": checkbox.group("mark").lower() == "x",
        }
    if not allow_plain:
        return None
    return {
        "text": body,
        "source": "bullet",
        "issue_checked": False,
    }


def _normalize_acceptance_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _normalize_acceptance_value_map(
    value_map: Mapping[str, Sequence[str]] | None,
) -> dict[str, tuple[str, ...]]:
    normalized: dict[str, tuple[str, ...]] = {}
    for key, values in dict(value_map or {}).items():
        normalized[_normalize_acceptance_key(key)] = tuple(
            str(value) for value in values if str(value)
        )
    return normalized


def _acceptance_values_for(
    criterion: Mapping[str, Any],
    value_map: Mapping[str, Sequence[str]],
) -> tuple[str, ...]:
    values: list[str] = []
    for key in _acceptance_lookup_keys(criterion):
        values.extend(value_map.get(key, ()))
    return tuple(dict.fromkeys(values))


def _acceptance_status_for(
    criterion: Mapping[str, Any],
    status_map: Mapping[str, str],
    *,
    files: Sequence[str],
    tests: Sequence[str],
    reports: Sequence[str],
) -> str:
    for key in _acceptance_lookup_keys(criterion):
        if key in status_map:
            return _normalize_acceptance_status(status_map[key])
    if criterion.get("issue_checked") is True:
        return "manual"
    if tests or reports:
        return "verified"
    if files:
        return "manual"
    return "missing"


def _acceptance_lookup_keys(criterion: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        _normalize_acceptance_key(str(criterion.get("id", ""))),
        _normalize_acceptance_key(str(criterion.get("index", ""))),
        _normalize_acceptance_key(
            f"criterion-{criterion.get('index', '')}",
        ),
        _normalize_acceptance_key(str(criterion.get("slug", ""))),
        _normalize_acceptance_key(str(criterion.get("text_hash", ""))),
        _normalize_acceptance_key(str(criterion.get("text_sha256", ""))),
        "*",
        "all",
    )


def _normalize_acceptance_key(value: object) -> str:
    return str(value).strip().lower().replace("_", "-")


def _normalize_acceptance_status(value: object) -> str:
    normalized = _normalize_acceptance_key(value)
    aliases = {
        "n/a": "not-applicable",
        "n-a": "not-applicable",
        "na": "not-applicable",
        "not-applicable": "not-applicable",
        "notapplicable": "not-applicable",
        "manual": "manual",
        "manually-asserted": "manual",
        "asserted": "manual",
        "verified": "verified",
        "verify": "verified",
        "pass": "verified",
        "passed": "verified",
        "missing": "missing",
    }
    status = aliases.get(normalized)
    if status not in ACCEPTANCE_STATUSES:
        raise ValueError(f"unsupported acceptance status: {value}")
    return status


def _safe_path_list(values: Sequence[str]) -> list[str]:
    return list(
        dict.fromkeys(publish_safe_path(str(value)) for value in values)
    )


def _safe_text_list(values: Sequence[str]) -> list[str]:
    safe_values = [
        publish_safe_json_value(str(value)) for value in values if str(value)
    ]
    return [str(value) for value in dict.fromkeys(safe_values)]


def _issue_report_payload(issue_state: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(issue_state)
    payload.pop("body", None)
    return payload


def collect_process_summary(
    repo_root: Path,
    *,
    runner: CommandRunner,
    rows: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Return bounded lingering process status without command lines."""
    if rows is None:
        result = runner(("ps", "-axo", "pid=,comm=,args="), repo_root)
        if result.returncode != 0:
            return {
                "state": "unavailable",
                "reason": _tail_text(result.stderr or result.stdout),
                "total_count": 0,
                "categories": {},
            }
        rows = tuple(result.stdout.splitlines())
    observations = tuple(_iter_process_observations(rows))
    categories: dict[str, list[ProcessObservation]] = defaultdict(list)
    for observation in observations:
        categories[observation.category].append(observation)
    category_payload = {
        category: {
            "count": len(items),
            "pids": [item.pid for item in items[:20]],
            "commands": sorted({item.command for item in items})[:10],
        }
        for category, items in sorted(
            categories.items(),
            key=lambda item: (
                PROCESS_CATEGORY_ORDER.index(item[0])
                if item[0] in PROCESS_CATEGORY_ORDER
                else len(PROCESS_CATEGORY_ORDER)
            ),
        )
    }
    return {
        "state": "clean" if not observations else "dirty",
        "total_count": len(observations),
        "categories": category_payload,
    }


def collect_artifact_summary(
    artifact_roots: Sequence[Path],
) -> dict[str, Any]:
    """Return publish-safe transient source artifact status."""
    roots = [
        source_artifact_cleanliness_payload(root, path_limit=25)
        for root in artifact_roots
    ]
    dirty_roots = [
        root
        for root in roots
        if int(root.get("source_artifact_count", 0) or 0) > 0
    ]
    return {
        "state": "clean" if not dirty_roots else "dirty",
        "root_count": len(roots),
        "dirty_root_count": len(dirty_roots),
        "source_artifact_count": sum(
            int(root.get("source_artifact_count", 0) or 0) for root in roots
        ),
        "roots": roots,
    }


def collect_gate_summary(
    repo_root: Path,
    *,
    run_gates: bool,
    runner: CommandRunner,
) -> dict[str, Any]:
    """Run or summarize the required closure gates."""
    if not run_gates:
        return {
            "state": "not-run",
            "reason": "run with --run-gates to execute closure gates",
            "required": [gate.display for gate in GATE_SPECS],
            "results": [],
        }
    results = [_run_gate(repo_root, gate, runner=runner) for gate in GATE_SPECS]
    return {
        "state": (
            "pass"
            if all(result["status"] == "pass" for result in results)
            else "fail"
        ),
        "results": results,
    }


def collect_release_preflight(
    repo_root: Path,
    *,
    run: bool,
    runner: CommandRunner,
) -> dict[str, Any]:
    """Run or explain TestPyPI/simple-registry release preflight status."""
    if not run:
        return {
            "state": "not-applicable",
            "reason": (
                "TestPyPI local simple-registry preflight is release-only; "
                "run with --release-preflight before publishing."
            ),
            "command": RELEASE_PREFLIGHT_GATE.display,
        }
    result = _run_gate(repo_root, RELEASE_PREFLIGHT_GATE, runner=runner)
    return {
        "state": result["status"],
        "result": result,
    }


def determine_readiness(
    *,
    git_state: Mapping[str, Any],
    issue_state: Mapping[str, Any],
    process_summary: Mapping[str, Any],
    artifact_summary: Mapping[str, Any],
    gates: Mapping[str, Any],
    release_preflight: Mapping[str, Any],
) -> dict[str, Any]:
    """Return final readiness state and blocking checks."""
    blockers: list[str] = []
    warnings: list[str] = []
    if git_state.get("dirty"):
        blockers.append("dirty-worktree")
    if git_state.get("upstream_state") != "aligned":
        blockers.append("upstream-not-aligned")
    if issue_state.get("state") == "unavailable":
        warnings.append("issue-state-unavailable")
    if process_summary.get("state") == "dirty":
        blockers.append("lingering-processes")
    if artifact_summary.get("state") == "dirty":
        blockers.append("transient-source-artifacts")
    if gates.get("state") == "not-run":
        blockers.append("gates-not-run")
    elif gates.get("state") != "pass":
        for gate in gates.get("results", []):
            if isinstance(gate, Mapping) and gate.get("status") != "pass":
                blockers.append(f"gate:{gate.get('name', 'unknown')}")
    if release_preflight.get("state") not in {"not-applicable", "pass"}:
        blockers.append("release-preflight")
    return {
        "state": "ready" if not blockers else "blocked",
        "blocking_checks": blockers,
        "warnings": warnings,
    }


def determine_precheck(
    *,
    git_state: Mapping[str, Any],
    issue_state: Mapping[str, Any],
    process_summary: Mapping[str, Any],
    artifact_summary: Mapping[str, Any],
    issue: int | None,
) -> dict[str, Any]:
    """Return cheap pre-gate readiness for running closure gates."""
    blockers: list[str] = []
    warnings: list[str] = []
    if git_state.get("dirty"):
        blockers.append("dirty-worktree")
    if git_state.get("upstream_state") != "aligned":
        blockers.append("upstream-not-aligned")
    if issue_state.get("state") == "unavailable":
        warnings.append("issue-state-unavailable")
    if process_summary.get("state") == "dirty":
        blockers.append("lingering-processes")
    if artifact_summary.get("state") == "dirty":
        blockers.append("transient-source-artifacts")
    state = "ready" if not blockers else "blocked"
    next_command = "python scripts/closure_readiness.py --run-gates"
    if issue is not None:
        next_command = (
            "python scripts/closure_readiness.py "
            f"--issue {issue} --run-gates"
        )
    return {
        "state": state,
        "ready_to_run_gates": state == "ready",
        "blocking_checks": blockers,
        "warnings": warnings,
        "next_command": next_command,
    }


def determine_workflow(
    report: Mapping[str, Any],
    *,
    required_branch: str = DEFAULT_REQUIRED_BRANCH,
) -> dict[str, Any]:
    """Return guided closure workflow state for one report."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    precheck = _mapping(report.get("precheck"))
    readiness = _mapping(report.get("readiness"))
    gates = _mapping(report.get("gates"))
    blockers: list[str] = []
    warnings: list[str] = []
    if repo.get("branch") != required_branch:
        blockers.append("not-dev-branch")
    for blocker in list(precheck.get("blocking_checks", []) or []):
        if str(blocker) not in blockers:
            blockers.append(str(blocker))
    for warning in list(precheck.get("warnings", []) or []):
        if str(warning) not in warnings:
            warnings.append(str(warning))
    if gates.get("state") == "not-run":
        state = "blocked" if blockers else "ready-to-run-gates"
    else:
        for blocker in list(readiness.get("blocking_checks", []) or []):
            if str(blocker) not in blockers:
                blockers.append(str(blocker))
        for warning in list(readiness.get("warnings", []) or []):
            if str(warning) not in warnings:
                warnings.append(str(warning))
        state = "ready" if not blockers else "blocked"
    issue_number = _int(issue.get("number"))
    next_command = "python scripts/closure_readiness.py --workflow"
    if issue_number:
        next_command = (
            "python scripts/closure_readiness.py "
            f"--issue {issue_number} --workflow"
        )
    return {
        "state": state,
        "required_branch": required_branch,
        "blocking_checks": blockers,
        "warnings": warnings,
        "next_command": next_command,
    }


def determine_commit_readiness(
    *,
    mode: str,
    git_state: Mapping[str, Any],
    issue_state: Mapping[str, Any],
    changes: Mapping[str, Any],
    scope: Mapping[str, Any],
    commit_message: Mapping[str, Any],
    required_branch: str = DEFAULT_REQUIRED_BRANCH,
    expected_upstream: str = DEFAULT_EXPECTED_UPSTREAM,
) -> dict[str, Any]:
    """Return report-only readiness for committing or pushing."""
    blockers: list[str] = []
    warnings: list[str] = []
    if git_state.get("branch") != required_branch:
        blockers.append("not-dev-branch")
    if not git_state.get("upstream"):
        blockers.append("upstream-missing")
    elif expected_upstream and git_state.get("upstream") != expected_upstream:
        blockers.append("unexpected-upstream")
    if _int(git_state.get("behind")) > 0:
        blockers.append("upstream-behind")
    if issue_state.get("state") == "unavailable":
        warnings.append("issue-state-unavailable")
    if changes.get("state") == "unavailable":
        blockers.append("git-status-unavailable")

    if mode == "push":
        if changes.get("state") == "dirty":
            blockers.append("dirty-worktree")
        if _int(git_state.get("ahead")) <= 0:
            blockers.append("no-commits-to-push")
    else:
        if _int(git_state.get("ahead")) > 0:
            blockers.append("upstream-ahead")
        if _int(changes.get("changed_path_count")) <= 0:
            blockers.append("no-changes")
        if commit_message.get("state") == "missing":
            blockers.append("commit-message-missing")
        elif commit_message.get("state") != "valid":
            blockers.append("commit-message-invalid")
        if scope.get("state") == "dirty-unrelated":
            blockers.append("unrelated-changes")
        if (
            scope.get("state") == "not-declared"
            and changes.get("state") == "dirty"
        ):
            warnings.append("scope-not-declared")

    return {
        "state": "ready" if not blockers else "blocked",
        "blocking_checks": blockers,
        "warnings": warnings,
    }


def attach_workflow(
    report: Mapping[str, Any],
    *,
    required_branch: str = DEFAULT_REQUIRED_BRANCH,
) -> dict[str, Any]:
    """Return report with guided workflow state attached."""
    updated: dict[str, Any] = dict(report)
    updated["workflow"] = determine_workflow(
        report,
        required_branch=required_branch,
    )
    safe = publish_safe_json_value(updated)
    if not isinstance(safe, dict):
        raise TypeError("closure readiness report must be a JSON object")
    return dict(safe)


def close_issue_if_ready(
    report: Mapping[str, Any],
    *,
    repo_root: Path = PROJECT_ROOT,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Close the linked issue only when full closure readiness is ready."""
    command_runner = runner or _run_command
    root = repo_root.expanduser().resolve(strict=False)
    readiness = _mapping(report.get("readiness"))
    workflow = _mapping(report.get("workflow"))
    acceptance = _mapping(report.get("acceptance_coverage"))
    issue = _mapping(report.get("issue"))
    number = _int(issue.get("number"))
    if workflow and workflow.get("state") != "ready":
        return {
            "state": "refused",
            "reason": "guided closure workflow is not ready",
            "blocking_checks": list(workflow.get("blocking_checks", []) or []),
        }
    if readiness.get("state") != "ready":
        return {
            "state": "refused",
            "reason": "closure readiness is not ready",
            "blocking_checks": list(readiness.get("blocking_checks", []) or []),
        }
    acceptance_blockers = _acceptance_close_blockers(acceptance)
    if acceptance_blockers:
        return {
            "state": "refused",
            "reason": "acceptance coverage is missing required criteria",
            "blocking_checks": acceptance_blockers,
            "acceptance_coverage": dict(acceptance),
        }
    if number <= 0:
        return {
            "state": "refused",
            "reason": "--issue is required to close a GitHub issue",
        }
    if _issue_is_closed(issue):
        return {
            "state": "already-closed",
            "issue_after": collect_issue_state(
                root,
                number,
                runner=command_runner,
            ),
        }
    result = command_runner(
        (
            "gh",
            "issue",
            "close",
            str(number),
            "--comment",
            str(report.get("close_comment", "")),
        ),
        root,
    )
    issue_after = collect_issue_state(root, number, runner=command_runner)
    return {
        "state": (
            "closed"
            if result.returncode == 0 and _issue_is_closed(issue_after)
            else "failed"
        ),
        "command": f"gh issue close {number} --comment <generated close comment>",
        "returncode": result.returncode,
        "stdout_tail": _tail_text(result.stdout),
        "stderr_tail": _tail_text(result.stderr),
        "issue_after": issue_after,
    }


def attach_issue_close_action(
    report: Mapping[str, Any],
    *,
    repo_root: Path = PROJECT_ROOT,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Return report with an issue-close action and final readback attached."""
    updated: dict[str, Any] = dict(report)
    updated["issue_close"] = close_issue_if_ready(
        report,
        repo_root=repo_root,
        runner=runner,
    )
    safe = publish_safe_json_value(updated)
    if not isinstance(safe, dict):
        raise TypeError("closure readiness report must be a JSON object")
    return dict(safe)


def attach_report_paths(
    report: Mapping[str, Any],
    *,
    json_path: Path | None = None,
    markdown_path: Path | None = None,
    repo_root: Path = PROJECT_ROOT,
    default_json: bool = False,
    default_markdown: bool = False,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Return report with publish-safe report-output metadata attached."""
    command_runner = runner or _run_command
    outputs: dict[str, Any] = {}
    if json_path is not None:
        outputs["json"] = _report_path_payload(
            json_path,
            repo_root=repo_root,
            default=default_json,
            runner=command_runner,
        )
    if markdown_path is not None:
        outputs["markdown"] = _report_path_payload(
            markdown_path,
            repo_root=repo_root,
            default=default_markdown,
            runner=command_runner,
        )
    if not outputs:
        return dict(report)
    updated = dict(report)
    updated["report_paths"] = outputs
    updated = _apply_report_path_readiness(updated)
    updated["close_comment"] = render_close_comment(updated)
    safe = publish_safe_json_value(updated)
    if not isinstance(safe, dict):
        raise TypeError("closure readiness report must be a JSON object")
    return dict(safe)


def load_readiness_report(path: Path) -> dict[str, Any]:
    """Load a publish-safe readiness report from disk."""
    payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise TypeError("closure readiness report must be a JSON object")
    safe = publish_safe_json_value(payload)
    if not isinstance(safe, dict):
        raise TypeError("closure readiness report must be a JSON object")
    return dict(safe)


def summarize_readiness_report(
    report: Mapping[str, Any],
) -> dict[str, Any]:
    """Return key report fields for quick operator readback."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    readiness = _mapping(report.get("readiness"))
    precheck = _mapping(report.get("precheck"))
    gates = _mapping(report.get("gates"))
    release = _mapping(report.get("release_preflight"))
    workflow = _mapping(report.get("workflow"))
    issue_close = _mapping(report.get("issue_close"))
    issue_after = _mapping(issue_close.get("issue_after"))
    report_paths = _mapping(report.get("report_paths"))
    acceptance = _mapping(report.get("acceptance_coverage"))
    close_state = str(issue_close.get("state", "not-run"))
    ready = readiness.get("state") == "ready"
    close_ok = close_state in {"not-run", "closed", "already-closed"}
    workflow_state = str(workflow.get("state", "not-run"))
    workflow_ok = not workflow or workflow_state == "ready"
    acceptance_ok = _acceptance_close_ready(acceptance)
    payload: dict[str, Any] = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "source_schema_version": report.get("schema_version", ""),
        "generated_at_utc": report.get("generated_at_utc", ""),
        "accepted": bool(ready and close_ok and workflow_ok and acceptance_ok),
        "readiness": {
            "state": readiness.get("state", "unknown"),
            "blocking_checks": list(readiness.get("blocking_checks", []) or []),
        },
        "precheck": {
            "state": precheck.get("state", "unknown"),
            "blocking_checks": list(precheck.get("blocking_checks", []) or []),
        },
        "gates": {
            "state": gates.get("state", "unknown"),
            "labels": _gate_labels(gates),
        },
        "release_preflight": {
            "state": release.get("state", "unknown"),
        },
        "workflow": {
            "state": workflow_state,
            "blocking_checks": list(workflow.get("blocking_checks", []) or []),
        },
        "repo": {
            "branch": repo.get("branch", "unknown"),
            "upstream": repo.get("upstream", ""),
            "ahead": repo.get("ahead", 0),
            "behind": repo.get("behind", 0),
            "dirty": bool(repo.get("dirty")),
            "head_short": repo.get("head_short", ""),
        },
        "issue": {
            "label": _issue_label(issue),
            "state": issue.get("state", "unknown"),
            "title": issue.get("title", ""),
            "url": issue.get("url", ""),
        },
        "issue_close": {
            "state": close_state,
            "final_issue": _issue_label(issue_after) if issue_after else "",
        },
        "acceptance_coverage": _acceptance_summary(acceptance),
        "report_paths": _report_paths_summary(report_paths),
    }
    safe = publish_safe_json_value(payload)
    if not isinstance(safe, dict):
        raise TypeError("closure readiness summary must be a JSON object")
    return dict(safe)


def summarize_issue_workflow_report(
    report: Mapping[str, Any],
) -> dict[str, Any]:
    """Return key fields from an executable issue workflow report."""
    readiness = _mapping(report.get("readiness"))
    commit_report = _mapping(report.get("commit_readiness"))
    push_report = _mapping(report.get("push_readiness"))
    final = _mapping(report.get("final_readback"))
    final_repo = _mapping(final.get("repo")) or _mapping(
        push_report.get("repo")
    ) or _mapping(commit_report.get("repo"))
    final_issue = _mapping(final.get("issue")) or _mapping(
        push_report.get("issue")
    ) or _mapping(commit_report.get("issue"))
    final_commit = _mapping(final.get("commit"))
    pre_mutation = _mapping(report.get("pre_mutation_gates"))
    closure = _mapping(report.get("closure_summary"))
    closure_readiness = _mapping(closure.get("readiness"))
    closure_gates = _mapping(closure.get("gates"))
    issue_close = _mapping(closure.get("issue_close"))
    acceptance = _mapping(report.get("acceptance_coverage"))
    report_paths = _report_paths_summary(_mapping(report.get("report_paths")))
    process_before = _process_summary(_mapping(report.get("processes_before")))
    process_after = _process_summary(_mapping(report.get("processes_after")))
    workflow_progress = _workflow_progress_summary(
        _mapping(report.get("workflow_progress"))
    )
    logs = _mapping(report.get("logs"))
    pre_mutation_state = str(pre_mutation.get("state", "not-run"))
    pre_mutation_enabled = bool(pre_mutation.get("enabled"))
    pre_mutation_ok = pre_mutation_state == "pass" or (
        pre_mutation_state == "not-run" and not pre_mutation_enabled
    )
    final_issue_ok = (
        not final_issue
        or str(final_issue.get("state", "")).upper() == "CLOSED"
    )
    repo_ok = (
        not bool(final_repo.get("dirty"))
        and _int(final_repo.get("ahead")) == 0
        and _int(final_repo.get("behind")) == 0
    )
    closure_ok = closure.get("accepted") is True
    issue_close_ok = str(issue_close.get("state", "not-run")) in {
        "closed",
        "already-closed",
    }
    process_ok = process_after.get("state") in {"clean", "not-recorded"}
    report_paths_ok = report_paths.get("state") != "blocked"
    accepted = bool(
        readiness.get("state") == "ready"
        and final_issue_ok
        and repo_ok
        and pre_mutation_ok
        and closure_ok
        and issue_close_ok
        and _acceptance_close_ready(acceptance)
        and process_ok
        and report_paths_ok
    )
    payload: dict[str, Any] = {
        "schema_version": ISSUE_WORKFLOW_SUMMARY_SCHEMA_VERSION,
        "source_schema_version": report.get("schema_version", ""),
        "generated_at_utc": report.get("generated_at_utc", ""),
        "accepted": accepted,
        "readiness": {
            "state": readiness.get("state", "unknown"),
            "blocking_checks": list(readiness.get("blocking_checks", []) or []),
            "warnings": list(readiness.get("warnings", []) or []),
        },
        "issue": {
            "label": _issue_label(final_issue)
            if final_issue
            else f"#{report.get('issue_number', '')}",
            "number": final_issue.get("number", report.get("issue_number", 0)),
            "state": final_issue.get("state", "unknown"),
            "title": final_issue.get("title", ""),
            "url": final_issue.get("url", ""),
        },
        "repo": {
            "branch": final_repo.get("branch", "unknown"),
            "upstream": final_repo.get("upstream", ""),
            "upstream_state": final_repo.get("upstream_state", "unknown"),
            "ahead": final_repo.get("ahead", 0),
            "behind": final_repo.get("behind", 0),
            "dirty": bool(final_repo.get("dirty")),
            "head_short": final_repo.get("head_short", ""),
        },
        "commit": {
            "state": final_commit.get("state", "unknown"),
            "summary": final_commit.get("summary", ""),
            "head_short": final_repo.get("head_short", ""),
        },
        "pre_mutation_gates": {
            "enabled": pre_mutation_enabled,
            "state": pre_mutation_state,
            "blocking_checks": list(
                pre_mutation.get("blocking_checks", []) or []
            ),
            "changed_paths_after": list(
                pre_mutation.get("changed_paths_after", []) or []
            ),
        },
        "acceptance_coverage": _acceptance_summary(acceptance),
        "closure": {
            "accepted": bool(closure.get("accepted")),
            "readiness_state": closure_readiness.get("state", "unknown"),
            "gates_state": closure_gates.get("state", "unknown"),
            "gate_labels": closure_gates.get("labels", ""),
            "issue_close_state": issue_close.get("state", "not-run"),
            "final_issue": issue_close.get("final_issue", ""),
        },
        "report_paths": report_paths,
        "process_health": {
            "before": process_before,
            "after": process_after,
        },
        "workflow_progress": workflow_progress,
        "logs": {
            "directory": logs.get("directory", ""),
            "command_count": logs.get(
                "command_count",
                len(report.get("commands", []) or []),
            ),
        },
    }
    safe = publish_safe_json_value(payload)
    if not isinstance(safe, dict):
        raise TypeError("issue workflow summary must be a JSON object")
    return dict(safe)


def summarize_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """Return the matching saved-report summary for the source schema."""
    if report.get("schema_version") == ISSUE_WORKFLOW_SCHEMA_VERSION:
        return summarize_issue_workflow_report(report)
    return summarize_readiness_report(report)


def render_saved_report_markdown(report: Mapping[str, Any]) -> str:
    """Render Markdown for a saved report using its source schema."""
    if report.get("schema_version") == ISSUE_WORKFLOW_SCHEMA_VERSION:
        return render_issue_workflow_markdown(report)
    return render_markdown(report)


def saved_report_close_comment(report: Mapping[str, Any]) -> str:
    """Return the publish-safe close comment embedded in a saved report."""
    if report.get("schema_version") == ISSUE_WORKFLOW_SCHEMA_VERSION:
        return str(_mapping(report.get("closure_report")).get("close_comment", ""))
    return str(report.get("close_comment", ""))


def _process_summary(processes: Mapping[str, Any]) -> dict[str, Any]:
    if not processes:
        return {
            "state": "not-recorded",
            "total_count": 0,
            "categories": {},
        }
    return {
        "state": processes.get("state", "unknown"),
        "total_count": processes.get("total_count", 0),
        "categories": dict(_mapping(processes.get("categories"))),
    }


def _workflow_progress_summary(
    progress: Mapping[str, Any],
) -> dict[str, Any]:
    if not progress:
        return {
            "state": "not-recorded",
            "stream": "not-recorded",
            "phase_count": 0,
            "event_count": 0,
            "elapsed_seconds": 0.0,
            "last_completed_phase": "",
        }
    return {
        "state": progress.get("state", "unknown"),
        "stream": progress.get("stream", "unknown"),
        "phase_count": progress.get("phase_count", 0),
        "event_count": progress.get("event_count", 0),
        "elapsed_seconds": progress.get("elapsed_seconds", 0.0),
        "last_completed_phase": progress.get("last_completed_phase", ""),
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    """Render a publish-safe Markdown readiness report."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    readiness = _mapping(report.get("readiness"))
    precheck = _mapping(report.get("precheck"))
    processes = _mapping(report.get("processes"))
    artifacts = _mapping(report.get("source_artifacts"))
    gates = _mapping(report.get("gates"))
    release = _mapping(report.get("release_preflight"))
    issue_close = _mapping(report.get("issue_close"))
    workflow = _mapping(report.get("workflow"))
    report_paths = _mapping(report.get("report_paths"))
    acceptance = _mapping(report.get("acceptance_coverage"))
    lines = [
        "# Closure Readiness Report",
        "",
        f"- State: **{readiness.get('state', 'unknown')}**",
        f"- Branch: `{repo.get('branch', 'unknown')}`",
        f"- Upstream: `{repo.get('upstream', '')}` "
        f"(ahead {repo.get('ahead', 0)}, behind {repo.get('behind', 0)})",
        f"- Commit: `{repo.get('head_short', '')}`",
        f"- Issue: {_issue_label(issue)}",
        f"- Generated: `{report.get('generated_at_utc', '')}`",
        "",
        "## Blocking Checks",
        "",
    ]
    blockers = list(readiness.get("blocking_checks", []) or [])
    if blockers:
        lines.extend(f"- `{blocker}`" for blocker in blockers)
    else:
        lines.append("- None")
    lines.extend(_render_acceptance_markdown(acceptance))
    lines.extend(
        [
            "",
            "## Gates",
            "",
            "| Gate | Status | Return code |",
            "| --- | --- | ---: |",
        ]
    )
    gate_results = list(gates.get("results", []) or [])
    if gate_results:
        for gate in gate_results:
            gate_map = _mapping(gate)
            lines.append(
                f"| `{gate_map.get('name', '')}` | "
                f"{gate_map.get('status', '')} | "
                f"{gate_map.get('returncode', '')} |"
            )
    else:
        lines.append(f"| required gates | {gates.get('state', 'unknown')} |  |")
    lines.extend(
        [
            "",
            "## Local State",
            "",
            f"- Precheck: {precheck.get('state', 'unknown')}",
            f"- Worktree dirty: {_yes_no(repo.get('dirty'))}",
            f"- Lingering processes: {processes.get('state', 'unknown')} "
            f"({processes.get('total_count', 0)})",
            f"- Transient source artifacts: {artifacts.get('state', 'unknown')} "
            f"({artifacts.get('source_artifact_count', 0)})",
            f"- Release preflight: {release.get('state', 'unknown')}",
            "",
        ]
    )
    if issue_close:
        lines.extend(
            [
                "## GitHub Close Action",
                "",
                f"- State: {issue_close.get('state', 'unknown')}",
                f"- Final issue: {_issue_label(_mapping(issue_close.get('issue_after')))}",
                "",
            ]
        )
    if workflow:
        lines.extend(
            [
                "## Guided Workflow",
                "",
                f"- State: {workflow.get('state', 'unknown')}",
                f"- Required branch: `{workflow.get('required_branch', '')}`",
                f"- Next: `{workflow.get('next_command', '')}`",
                "",
            ]
        )
    if report_paths:
        lines.extend(["## Report Paths", ""])
        lines.extend(
            [
                "| Output | Path | Kind | Git ignore | Effect | Write |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for kind, payload in report_paths.items():
            payload_map = _mapping(payload)
            default_text = "default" if payload_map.get("default") else "custom"
            write_text = (
                "will write"
                if payload_map.get("write_allowed", True)
                else "skipped"
            )
            lines.append(
                f"| {kind} | `{payload_map.get('path', '')}` | "
                f"{default_text} | "
                f"{payload_map.get('gitignore_state', 'unknown')} | "
                f"{payload_map.get('workspace_effect', 'unknown')} | "
                f"{write_text} |"
            )
        lines.append("")
    lines.extend(
        [
            "## GitHub Close Comment",
            "",
            "```text",
            str(report.get("close_comment", "")),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def render_issue_audit_human(report: Mapping[str, Any]) -> str:
    """Render a compact issue/local readback without closure gates."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    precheck = _mapping(report.get("precheck"))
    acceptance = _mapping(report.get("acceptance_coverage"))
    lines = [
        "Closure issue audit",
        f"issue: {_issue_label(issue)}",
        f"title: {issue.get('title', '')}",
        f"url: {issue.get('url', '')}",
        f"branch: {repo.get('branch', 'unknown')}",
        f"upstream: {repo.get('upstream', '')} "
        f"ahead={repo.get('ahead', 0)} behind={repo.get('behind', 0)}",
        f"commit: {repo.get('head_short', '')}",
        f"worktree dirty: {_yes_no(repo.get('dirty'))}",
        f"precheck: {precheck.get('state', 'unknown')}",
        _acceptance_human_line(acceptance),
    ]
    blockers = list(precheck.get("blocking_checks", []) or [])
    if blockers:
        lines.append("blocking: " + ", ".join(str(item) for item in blockers))
    next_command = precheck.get("next_command", "")
    if next_command:
        lines.append(f"next: {next_command}")
    return "\n".join(lines)


def render_precheck_human(report: Mapping[str, Any]) -> str:
    """Render a compact pre-gate console summary."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    precheck = _mapping(report.get("precheck"))
    lines = [
        "Closure precheck",
        f"state: {precheck.get('state', 'unknown')}",
        f"issue: {_issue_label(issue)}",
        f"branch: {repo.get('branch', 'unknown')}",
        f"upstream: {repo.get('upstream', '')} "
        f"ahead={repo.get('ahead', 0)} behind={repo.get('behind', 0)}",
        f"worktree dirty: {_yes_no(repo.get('dirty'))}",
        f"next: {precheck.get('next_command', '')}",
    ]
    blockers = list(precheck.get("blocking_checks", []) or [])
    if blockers:
        lines.append("blocking: " + ", ".join(str(item) for item in blockers))
    return "\n".join(lines)


def render_commit_readiness_human(report: Mapping[str, Any]) -> str:
    """Render a compact commit/push readiness console summary."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    changes = _mapping(report.get("changes"))
    scope = _mapping(report.get("scope"))
    message = _mapping(report.get("commit_message"))
    readiness = _mapping(report.get("readiness"))
    plan = list(report.get("command_plan", []) or [])
    mode = str(report.get("mode", "commit"))
    lines = [
        f"{mode.title()} readiness",
        f"state: {readiness.get('state', 'unknown')}",
        f"issue: {_issue_label(issue)}",
        f"branch: {repo.get('branch', 'unknown')}",
        f"upstream: {repo.get('upstream', '')} "
        f"ahead={repo.get('ahead', 0)} behind={repo.get('behind', 0)}",
        f"commit: {repo.get('head_short', '')}",
        f"worktree: {changes.get('state', 'unknown')}",
        (
            "changes: "
            f"{changes.get('changed_path_count', 0)} total, "
            f"{changes.get('staged_count', 0)} staged, "
            f"{changes.get('unstaged_count', 0)} unstaged, "
            f"{changes.get('untracked_count', 0)} untracked"
        ),
        f"scope: {scope.get('state', 'not-declared')}",
    ]
    if mode == "commit":
        lines.append(f"message: {message.get('state', 'not-checked')}")
    blockers = list(readiness.get("blocking_checks", []) or [])
    if blockers:
        lines.append("blocking: " + ", ".join(str(item) for item in blockers))
    warnings = list(readiness.get("warnings", []) or [])
    if warnings:
        lines.append("warnings: " + ", ".join(str(item) for item in warnings))
    unrelated = list(scope.get("unrelated_paths", []) or [])
    if unrelated:
        lines.append("unrelated: " + ", ".join(str(item) for item in unrelated))
    if plan:
        lines.append("next:")
        for command in plan:
            lines.append(f"  {command}")
    return "\n".join(lines)


def render_human(report: Mapping[str, Any]) -> str:
    """Render a compact console summary."""
    repo = _mapping(report.get("repo"))
    readiness = _mapping(report.get("readiness"))
    precheck = _mapping(report.get("precheck"))
    gates = _mapping(report.get("gates"))
    issue = _mapping(report.get("issue"))
    issue_close = _mapping(report.get("issue_close"))
    workflow = _mapping(report.get("workflow"))
    report_paths = _mapping(report.get("report_paths"))
    acceptance = _mapping(report.get("acceptance_coverage"))
    lines = [
        "Closure readiness",
        f"state: {readiness.get('state', 'unknown')}",
        f"precheck: {precheck.get('state', 'unknown')}",
        f"issue: {_issue_label(issue)}",
        f"branch: {repo.get('branch', 'unknown')}",
        f"upstream: {repo.get('upstream', '')} "
        f"ahead={repo.get('ahead', 0)} behind={repo.get('behind', 0)}",
        f"commit: {repo.get('head_short', '')}",
        f"worktree dirty: {_yes_no(repo.get('dirty'))}",
        f"gates: {gates.get('state', 'unknown')}",
        _acceptance_human_line(acceptance),
    ]
    blockers = list(readiness.get("blocking_checks", []) or [])
    if blockers:
        lines.append("blocking: " + ", ".join(str(item) for item in blockers))
    if issue_close:
        lines.append(f"issue close: {issue_close.get('state', 'unknown')}")
        issue_after = _mapping(issue_close.get("issue_after"))
        if issue_after:
            lines.append(f"issue final: {_issue_label(issue_after)}")
    if workflow:
        lines.append(f"workflow: {workflow.get('state', 'unknown')}")
        workflow_blockers = list(workflow.get("blocking_checks", []) or [])
        if workflow_blockers:
            lines.append(
                "workflow blocking: "
                + ", ".join(str(item) for item in workflow_blockers)
            )
    if report_paths:
        lines.append("reports:")
        for kind, payload in report_paths.items():
            payload_map = _mapping(payload)
            write_state = (
                "write" if payload_map.get("write_allowed", True) else "skip"
            )
            lines.append(
                f"  {kind}: {payload_map.get('path', '')} "
                f"[{payload_map.get('gitignore_state', 'unknown')}; "
                f"{write_state}]"
            )
    return "\n".join(lines)


def render_report_summary_human(summary: Mapping[str, Any]) -> str:
    """Render a compact summary for a saved closure report."""
    repo = _mapping(summary.get("repo"))
    issue = _mapping(summary.get("issue"))
    readiness = _mapping(summary.get("readiness"))
    precheck = _mapping(summary.get("precheck"))
    gates = _mapping(summary.get("gates"))
    release = _mapping(summary.get("release_preflight"))
    workflow = _mapping(summary.get("workflow"))
    issue_close = _mapping(summary.get("issue_close"))
    report_paths = _mapping(summary.get("report_paths"))
    acceptance = _mapping(summary.get("acceptance_coverage"))
    lines = [
        "Closure report summary",
        f"accepted: {_yes_no(summary.get('accepted'))}",
        f"state: {readiness.get('state', 'unknown')}",
        f"precheck: {precheck.get('state', 'unknown')}",
        f"gates: {gates.get('state', 'unknown')}",
        f"issue: {issue.get('label', '')}",
        f"branch: {repo.get('branch', 'unknown')} -> "
        f"{repo.get('upstream', '')} "
        f"ahead={repo.get('ahead', 0)} behind={repo.get('behind', 0)}",
        f"commit: {repo.get('head_short', '')}",
        f"worktree dirty: {_yes_no(repo.get('dirty'))}",
        f"release preflight: {release.get('state', 'unknown')}",
        f"workflow: {workflow.get('state', 'not-run')}",
        _acceptance_summary_human_line(acceptance),
        f"report paths: {report_paths.get('state', 'not-recorded')}",
        f"issue close: {issue_close.get('state', 'not-run')}",
    ]
    if issue_close.get("final_issue"):
        lines.append(f"issue final: {issue_close.get('final_issue')}")
    if summary.get("generated_at_utc"):
        lines.append(f"generated: {summary.get('generated_at_utc')}")
    blockers = list(readiness.get("blocking_checks", []) or [])
    if blockers:
        lines.append("blocking: " + ", ".join(str(item) for item in blockers))
    workflow_blockers = list(workflow.get("blocking_checks", []) or [])
    if workflow_blockers:
        lines.append(
            "workflow blocking: "
            + ", ".join(str(item) for item in workflow_blockers)
        )
    return "\n".join(lines)


def render_issue_workflow_summary_human(summary: Mapping[str, Any]) -> str:
    """Render a compact summary for a saved executable issue workflow."""
    repo = _mapping(summary.get("repo"))
    issue = _mapping(summary.get("issue"))
    readiness = _mapping(summary.get("readiness"))
    commit = _mapping(summary.get("commit"))
    pre_mutation = _mapping(summary.get("pre_mutation_gates"))
    closure = _mapping(summary.get("closure"))
    report_paths = _mapping(summary.get("report_paths"))
    acceptance = _mapping(summary.get("acceptance_coverage"))
    process_health = _mapping(summary.get("process_health"))
    process_after = _mapping(process_health.get("after"))
    workflow_progress = _mapping(summary.get("workflow_progress"))
    logs = _mapping(summary.get("logs"))
    lines = [
        "Issue workflow report summary",
        f"accepted: {_yes_no(summary.get('accepted'))}",
        f"state: {readiness.get('state', 'unknown')}",
        f"issue: {issue.get('label', '')}",
        f"branch: {repo.get('branch', 'unknown')} -> "
        f"{repo.get('upstream', '')} "
        f"({repo.get('upstream_state', 'unknown')}) "
        f"ahead={repo.get('ahead', 0)} behind={repo.get('behind', 0)}",
        f"commit: {commit.get('summary') or repo.get('head_short', '')}",
        f"worktree dirty: {_yes_no(repo.get('dirty'))}",
        f"pre-mutation gates: {pre_mutation.get('state', 'not-run')}",
        f"closure: {'accepted' if closure.get('accepted') else 'blocked'}",
        f"issue close: {closure.get('issue_close_state', 'not-run')}",
        _acceptance_summary_human_line(acceptance),
        f"report paths: {report_paths.get('state', 'not-recorded')}",
        (
            "runtime/process health: "
            f"{process_after.get('state', 'not-recorded')} "
            f"({process_after.get('total_count', 0)})"
        ),
        (
            "progress: "
            f"{workflow_progress.get('state', 'not-recorded')} "
            f"({workflow_progress.get('phase_count', 0)} phases, "
            f"{workflow_progress.get('elapsed_seconds', 0.0)}s)"
        ),
        (
            "logs: "
            f"{logs.get('directory', '')} "
            f"({logs.get('command_count', 0)} commands)"
        ),
    ]
    if summary.get("generated_at_utc"):
        lines.append(f"generated: {summary.get('generated_at_utc')}")
    blockers = list(readiness.get("blocking_checks", []) or [])
    if blockers:
        lines.append("blocking: " + ", ".join(str(item) for item in blockers))
    warnings = list(readiness.get("warnings", []) or [])
    if warnings:
        lines.append("warnings: " + ", ".join(str(item) for item in warnings))
    changed_paths = list(pre_mutation.get("changed_paths_after", []) or [])
    if changed_paths:
        lines.append(
            "pre-mutation changed: "
            + ", ".join(str(item) for item in changed_paths)
        )
    if closure.get("final_issue"):
        lines.append(f"issue final: {closure.get('final_issue')}")
    outputs = _mapping(report_paths.get("outputs"))
    if outputs:
        lines.append("reports:")
        for kind, payload in outputs.items():
            payload_map = _mapping(payload)
            write_state = (
                "write" if payload_map.get("write_allowed", True) else "skip"
            )
            lines.append(
                f"  {kind}: {payload_map.get('path', '')} "
                f"[{payload_map.get('gitignore_state', 'unknown')}; "
                f"{write_state}]"
            )
    return "\n".join(lines)


def render_summary_human(summary: Mapping[str, Any]) -> str:
    """Render a saved-report summary with the matching human format."""
    if summary.get("schema_version") == ISSUE_WORKFLOW_SUMMARY_SCHEMA_VERSION:
        return render_issue_workflow_summary_human(summary)
    return render_report_summary_human(summary)


def render_close_comment(report: Mapping[str, Any]) -> str:
    """Return a concise issue-close evidence block."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    readiness = _mapping(report.get("readiness"))
    gates = _mapping(report.get("gates"))
    processes = _mapping(report.get("processes"))
    artifacts = _mapping(report.get("source_artifacts"))
    release = _mapping(report.get("release_preflight"))
    acceptance = _mapping(report.get("acceptance_coverage"))
    gate_labels = _gate_labels(gates)
    lines = [
        f"Closure readiness: {readiness.get('state', 'unknown')}",
        f"Issue: {_issue_label(issue)}",
        f"Commit: {repo.get('head', '')}",
        f"Branch: {repo.get('branch', 'unknown')} -> "
        f"{repo.get('upstream', '')} "
        f"(ahead {repo.get('ahead', 0)}, behind {repo.get('behind', 0)})",
        f"Worktree clean: {_yes_no(not bool(repo.get('dirty')))}",
        f"Gates: {gate_labels}",
        f"Lingering processes: {processes.get('state', 'unknown')} "
        f"({processes.get('total_count', 0)})",
        f"Transient source artifacts: {artifacts.get('state', 'unknown')} "
        f"({artifacts.get('source_artifact_count', 0)})",
        f"Release preflight: {release.get('state', 'unknown')}",
        _acceptance_close_comment_line(acceptance),
    ]
    missing_ids = list(acceptance.get("missing_ids", []) or [])
    if missing_ids:
        lines.append("Missing acceptance criteria: " + ", ".join(missing_ids))
    override = _mapping(acceptance.get("override"))
    if override.get("missing_ok"):
        lines.append(
            "Acceptance override: "
            + str(override.get("reason", "operator override supplied"))
        )
    if readiness.get("blocking_checks"):
        lines.append(
            "Blocking checks: "
            + ", ".join(str(item) for item in readiness["blocking_checks"])
        )
    return "\n".join(lines)


def _acceptance_close_blockers(
    acceptance: Mapping[str, Any],
) -> list[str]:
    if not acceptance:
        return []
    if acceptance.get("state") != "blocked":
        return []
    blockers = [ACCEPTANCE_MISSING_BLOCKER]
    blockers.extend(
        f"acceptance:{item}"
        for item in list(acceptance.get("missing_ids", []) or [])
    )
    return blockers


def _acceptance_close_ready(acceptance: Mapping[str, Any]) -> bool:
    if not acceptance:
        return True
    return str(acceptance.get("state", "")) in {
        "ready",
        "not-applicable",
        "not-requested",
        "override",
    }


def _acceptance_summary(acceptance: Mapping[str, Any]) -> dict[str, Any]:
    if not acceptance:
        return {
            "state": "not-recorded",
            "criteria_count": 0,
            "covered_count": 0,
            "missing_count": 0,
            "missing_ids": [],
            "override": {"missing_ok": False, "reason": ""},
        }
    override = _mapping(acceptance.get("override"))
    return {
        "state": acceptance.get("state", "unknown"),
        "criteria_count": acceptance.get("criteria_count", 0),
        "covered_count": acceptance.get("covered_count", 0),
        "missing_count": acceptance.get("missing_count", 0),
        "missing_ids": list(acceptance.get("missing_ids", []) or []),
        "override": {
            "missing_ok": bool(override.get("missing_ok")),
            "reason": str(override.get("reason", "")),
        },
    }


def _acceptance_human_line(acceptance: Mapping[str, Any]) -> str:
    summary = _acceptance_summary(acceptance)
    return (
        "acceptance: "
        f"{summary.get('state', 'unknown')} "
        f"({summary.get('covered_count', 0)}/"
        f"{summary.get('criteria_count', 0)} covered, "
        f"{summary.get('missing_count', 0)} missing)"
    )


def _acceptance_summary_human_line(acceptance: Mapping[str, Any]) -> str:
    return _acceptance_human_line(acceptance)


def _acceptance_close_comment_line(acceptance: Mapping[str, Any]) -> str:
    summary = _acceptance_summary(acceptance)
    return (
        "Acceptance coverage: "
        f"{summary.get('state', 'unknown')} "
        f"({summary.get('covered_count', 0)}/"
        f"{summary.get('criteria_count', 0)} covered, "
        f"{summary.get('missing_count', 0)} missing)"
    )


def _render_acceptance_markdown(
    acceptance: Mapping[str, Any],
) -> list[str]:
    if not acceptance:
        return [
            "",
            "## Acceptance Coverage",
            "",
            "- State: not-recorded",
        ]
    lines = [
        "",
        "## Acceptance Coverage",
        "",
        f"- State: {acceptance.get('state', 'unknown')}",
        f"- Criteria: {acceptance.get('covered_count', 0)}/"
        f"{acceptance.get('criteria_count', 0)} covered",
        f"- Missing: {acceptance.get('missing_count', 0)}",
    ]
    override = _mapping(acceptance.get("override"))
    if override.get("missing_ok"):
        lines.append(f"- Override: {override.get('reason', '')}")
    items = list(acceptance.get("items", []) or [])
    if not items:
        lines.append("")
        lines.append("_No acceptance criteria found._")
        return lines
    lines.extend(
        [
            "",
            "| ID | Status | Criterion | Evidence |",
            "| --- | --- | --- | --- |",
        ]
    )
    for item in items:
        item_map = _mapping(item)
        evidence = _acceptance_evidence_cell(item_map)
        lines.append(
            f"| `{item_map.get('id', '')}` | "
            f"{item_map.get('status', '')} | "
            f"{_markdown_cell(str(item_map.get('text', '')))} | "
            f"{_markdown_cell(evidence)} |"
        )
    return lines


def _acceptance_evidence_cell(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for label, field in (
        ("files", "files"),
        ("tests", "tests"),
        ("reports", "reports"),
        ("notes", "notes"),
    ):
        values = [str(value) for value in list(item.get(field, []) or [])]
        if values:
            rendered = ", ".join(f"`{value}`" for value in values)
            parts.append(f"{label}: {rendered}")
    return "; ".join(parts) if parts else "-"


def _markdown_cell(value: str) -> str:
    return value.replace("\n", " ").replace("|", "\\|")


def _split_acceptance_spec(
    spec: str,
    *,
    parser: argparse.ArgumentParser,
    option: str,
) -> tuple[str, str]:
    if "=" not in spec:
        parser.error(f"{option} expects KEY=VALUE")
    key, value = spec.split("=", 1)
    key = _normalize_acceptance_key(key)
    value = value.strip()
    if not key or not value:
        parser.error(f"{option} expects non-empty KEY=VALUE")
    return key, value


def _parse_acceptance_status_specs(
    specs: Sequence[str],
    *,
    parser: argparse.ArgumentParser,
) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for spec in specs:
        key, value = _split_acceptance_spec(
            spec,
            parser=parser,
            option="--acceptance-status",
        )
        try:
            parsed[key] = _normalize_acceptance_status(value)
        except ValueError as exc:
            parser.error(str(exc))
    return parsed


def _parse_acceptance_value_specs(
    specs: Sequence[str],
    *,
    parser: argparse.ArgumentParser,
    option: str,
) -> dict[str, tuple[str, ...]]:
    parsed: dict[str, list[str]] = defaultdict(list)
    for spec in specs:
        key, value = _split_acceptance_spec(
            spec,
            parser=parser,
            option=option,
        )
        parsed[key].append(value)
    return {key: tuple(values) for key, values in parsed.items()}


def _acceptance_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "acceptance_statuses": args.acceptance_statuses,
        "acceptance_files": args.acceptance_files,
        "acceptance_tests": args.acceptance_tests,
        "acceptance_reports": args.acceptance_reports,
        "acceptance_notes": args.acceptance_notes,
        "acceptance_missing_ok": bool(args.acceptance_missing_ok),
        "acceptance_override_reason": str(
            args.acceptance_override_reason or "",
        ),
    }


def _all_acceptance_map(values: Sequence[str]) -> dict[str, tuple[str, ...]]:
    cleaned = tuple(str(value) for value in values if str(value))
    return {"*": cleaned} if cleaned else {}


def _merge_acceptance_value_maps(
    left: Mapping[str, Sequence[str]] | None,
    right: Mapping[str, Sequence[str]] | None,
) -> dict[str, tuple[str, ...]]:
    merged: dict[str, list[str]] = defaultdict(list)
    for mapping in (left or {}, right or {}):
        for key, values in mapping.items():
            normalized = _normalize_acceptance_key(key)
            for value in values:
                text = str(value)
                if text and text not in merged[normalized]:
                    merged[normalized].append(text)
    return {key: tuple(values) for key, values in merged.items()}


def _acceptance_tests_from_paths(paths: Sequence[str]) -> tuple[str, ...]:
    tests = []
    for path in paths:
        normalized = str(path).replace("\\", "/")
        if normalized.startswith("tests/"):
            tests.append(normalized)
    return tuple(dict.fromkeys(tests))


def _acceptance_report_paths(
    report_paths: Mapping[str, Any],
) -> tuple[str, ...]:
    paths: list[str] = []
    for payload in report_paths.values():
        payload_map = _mapping(payload)
        path = str(payload_map.get("path", ""))
        if path and payload_map.get("write_allowed", True):
            paths.append(path)
    return tuple(dict.fromkeys(paths))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", type=int, help="GitHub issue number to read")
    parser.add_argument(
        "--issue-audit",
        action="store_true",
        help="read back the linked issue and cheap local closure state",
    )
    parser.add_argument(
        "--commit-readiness",
        action="store_true",
        help="report whether current changes are ready to stage and commit",
    )
    parser.add_argument(
        "--push-readiness",
        action="store_true",
        help="report whether committed local changes are ready to push",
    )
    parser.add_argument(
        "--execute-workflow",
        "--execute-issue-workflow",
        action="store_true",
        help=(
            "execute the audited issue workflow: readiness, targeted commit, "
            "push, closure gates, issue close, and final readback"
        ),
    )
    parser.add_argument(
        "--pre-mutation-gates",
        action="store_true",
        help=(
            "with --execute-workflow, run closure gates before git add/commit/"
            "push and block if they fail or change files"
        ),
    )
    parser.add_argument(
        "--quiet-progress",
        "--no-progress",
        action="store_true",
        help=(
            "with --execute-workflow, suppress live stderr progress lines while "
            "still recording phase timing in the saved report"
        ),
    )
    parser.add_argument(
        "--commit-message",
        "--message",
        dest="commit_message",
        help="candidate Commitizen commit message to validate",
    )
    parser.add_argument(
        "--commit-message-file",
        type=Path,
        help="file containing the candidate Commitizen commit message",
    )
    parser.add_argument(
        "--commit-path",
        "--changed-path",
        dest="commit_paths",
        type=Path,
        action="append",
        help="intended changed path; repeat to declare the safe commit scope",
    )
    parser.add_argument(
        "--acceptance-status",
        dest="acceptance_status_specs",
        action="append",
        default=[],
        metavar="KEY=STATUS",
        help=(
            "set acceptance criterion status; KEY may be ac-001, 1, slug, "
            "hash, or *; STATUS is verified, manual, not-applicable, or missing"
        ),
    )
    parser.add_argument(
        "--acceptance-file",
        dest="acceptance_file_specs",
        action="append",
        default=[],
        metavar="KEY=PATH",
        help="attach a publish-safe touched file to an acceptance criterion",
    )
    parser.add_argument(
        "--acceptance-test",
        dest="acceptance_test_specs",
        action="append",
        default=[],
        metavar="KEY=TEST",
        help="attach a focused test or test command to an acceptance criterion",
    )
    parser.add_argument(
        "--acceptance-report",
        dest="acceptance_report_specs",
        action="append",
        default=[],
        metavar="KEY=PATH",
        help="attach a relevant report path to an acceptance criterion",
    )
    parser.add_argument(
        "--acceptance-note",
        dest="acceptance_note_specs",
        action="append",
        default=[],
        metavar="KEY=TEXT",
        help="attach a bounded manual note to an acceptance criterion",
    )
    parser.add_argument(
        "--acceptance-missing-ok",
        action="store_true",
        help=(
            "allow automatic issue close with missing acceptance criteria and "
            "record an explicit override"
        ),
    )
    parser.add_argument(
        "--acceptance-override-reason",
        default="",
        help="reason recorded when --acceptance-missing-ok is used",
    )
    parser.add_argument(
        "--required-branch",
        default=DEFAULT_REQUIRED_BRANCH,
        help="branch required by guided maintainer workflows; default dev",
    )
    parser.add_argument(
        "--expected-upstream",
        default=DEFAULT_EXPECTED_UPSTREAM,
        help="upstream branch required by maintainer workflows; default origin/dev",
    )
    parser.add_argument(
        "--precheck",
        action="store_true",
        help="run cheap local checks and report readiness to run closure gates",
    )
    parser.add_argument(
        "--run-gates",
        action="store_true",
        help="run pytest, pre-commit, help sync, diff check, and help smoke",
    )
    parser.add_argument(
        "--release-preflight",
        action="store_true",
        help="run the TestPyPI local simple-registry preflight",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        action="append",
        dest="artifact_roots",
        help="root to scan for transient ZIP/CSV/XLS/XLSX artifacts",
    )
    parser.add_argument("--report-json", type=Path, help="write JSON report")
    parser.add_argument(
        "--report-markdown",
        type=Path,
        help="write Markdown report",
    )
    parser.add_argument(
        "--write-reports",
        action="store_true",
        help="write issue-derived default JSON and Markdown reports",
    )
    parser.add_argument(
        "--summarize-report",
        "--read-report",
        type=Path,
        dest="summarize_report",
        help="summarize an existing closure JSON report without live commands",
    )
    parser.add_argument("--json", action="store_true", help="print JSON")
    parser.add_argument(
        "--full-json",
        action="store_true",
        help=(
            "with --execute-workflow, print the full evidence JSON instead of "
            "the compact closeout summary JSON"
        ),
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="print Markdown instead of the compact summary",
    )
    parser.add_argument(
        "--print-close-comment",
        action="store_true",
        help="print only the generated publish-safe close comment",
    )
    parser.add_argument(
        "--close-issue",
        action="store_true",
        help="close --issue with the generated comment when readiness is ready",
    )
    parser.add_argument(
        "--workflow",
        action="store_true",
        help=(
            "run guided closure workflow with precheck, gates, default reports, "
            "and optional explicit close"
        ),
    )
    args = parser.parse_args(argv)
    args.acceptance_statuses = _parse_acceptance_status_specs(
        args.acceptance_status_specs,
        parser=parser,
    )
    args.acceptance_files = _parse_acceptance_value_specs(
        args.acceptance_file_specs,
        parser=parser,
        option="--acceptance-file",
    )
    args.acceptance_tests = _parse_acceptance_value_specs(
        args.acceptance_test_specs,
        parser=parser,
        option="--acceptance-test",
    )
    args.acceptance_reports = _parse_acceptance_value_specs(
        args.acceptance_report_specs,
        parser=parser,
        option="--acceptance-report",
    )
    args.acceptance_notes = _parse_acceptance_value_specs(
        args.acceptance_note_specs,
        parser=parser,
        option="--acceptance-note",
    )
    if args.commit_readiness and args.push_readiness:
        parser.error(
            "--commit-readiness cannot be combined with --push-readiness"
        )
    if args.commit_message and args.commit_message_file:
        parser.error(
            "--commit-message cannot be combined with --commit-message-file"
        )
    if args.push_readiness and (
        args.commit_message or args.commit_message_file or args.commit_paths
    ):
        parser.error(
            "--push-readiness cannot be combined with commit message or path options"
        )
    change_readiness = args.commit_readiness or args.push_readiness
    if change_readiness and (
        args.issue_audit
        or args.execute_workflow
        or args.pre_mutation_gates
        or args.precheck
        or args.run_gates
        or args.release_preflight
        or args.artifact_roots
        or args.report_json
        or args.report_markdown
        or args.write_reports
        or args.summarize_report
        or args.full_json
        or args.markdown
        or args.print_close_comment
        or args.close_issue
        or args.workflow
        or args.acceptance_status_specs
        or args.acceptance_file_specs
        or args.acceptance_test_specs
        or args.acceptance_report_specs
        or args.acceptance_note_specs
        or args.acceptance_missing_ok
        or args.acceptance_override_reason
    ):
        parser.error(
            "--commit-readiness/--push-readiness can only be combined with "
            "--issue, --json, --required-branch, --expected-upstream, "
            "and commit-readiness options"
        )
    if args.execute_workflow:
        if args.issue is None:
            parser.error("--execute-workflow requires --issue")
        if not (args.commit_message or args.commit_message_file):
            parser.error("--execute-workflow requires --commit-message")
        if not args.commit_paths:
            parser.error(
                "--execute-workflow requires at least one --commit-path"
            )
        if (
            args.issue_audit
            or args.precheck
            or args.run_gates
            or args.workflow
            or args.close_issue
            or args.write_reports
            or args.summarize_report
            or args.print_close_comment
        ):
            parser.error(
                "--execute-workflow cannot be combined with issue audit, "
                "manual gate, guided workflow, close, report-write, summarize, "
                "or close-comment modes"
            )
    elif args.pre_mutation_gates:
        parser.error("--pre-mutation-gates requires --execute-workflow")
    if args.full_json and not args.execute_workflow:
        parser.error("--full-json requires --execute-workflow")
    if args.quiet_progress and not args.execute_workflow:
        parser.error("--quiet-progress requires --execute-workflow")
    if args.precheck and args.run_gates:
        parser.error("--precheck cannot be combined with --run-gates")
    if args.precheck and args.release_preflight:
        parser.error("--precheck cannot be combined with --release-preflight")
    if args.issue_audit and args.issue is None:
        parser.error("--issue-audit requires --issue")
    if args.issue_audit and (
        args.run_gates or args.release_preflight or args.close_issue
    ):
        parser.error(
            "--issue-audit cannot be combined with gates, release preflight, "
            "or issue close"
        )
    if args.workflow and args.issue is None:
        parser.error("--workflow requires --issue")
    if args.workflow and (args.precheck or args.issue_audit):
        parser.error(
            "--workflow cannot be combined with --precheck or --issue-audit"
        )
    if args.close_issue and args.issue is None:
        parser.error("--close-issue requires --issue")
    if args.write_reports and args.issue is None:
        parser.error("--write-reports requires --issue")
    if args.summarize_report and (
        args.issue is not None
        or args.issue_audit
        or args.commit_readiness
        or args.push_readiness
        or args.execute_workflow
        or args.precheck
        or args.run_gates
        or args.release_preflight
        or args.artifact_roots
        or args.report_json
        or args.report_markdown
        or args.write_reports
        or args.full_json
        or args.close_issue
        or args.workflow
        or args.acceptance_status_specs
        or args.acceptance_file_specs
        or args.acceptance_test_specs
        or args.acceptance_report_specs
        or args.acceptance_note_specs
        or args.acceptance_missing_ok
        or args.acceptance_override_reason
    ):
        parser.error("--summarize-report cannot be combined with live checks")
    output_modes = (
        args.json,
        args.full_json,
        args.markdown,
        args.print_close_comment,
    )
    if sum(1 for enabled in output_modes if enabled) > 1:
        parser.error(
            "choose only one of --json, --full-json, --markdown, or "
            "--print-close-comment"
        )
    return args


def main(
    argv: Sequence[str] | None = None,
    *,
    repo_root: Path = PROJECT_ROOT,
    runner: CommandRunner | None = None,
) -> int:
    """Run the closure-readiness helper."""
    args = parse_args(argv)
    root = repo_root.expanduser().resolve(strict=False)
    if args.summarize_report:
        report_path = _output_path(args.summarize_report, root)
        report = load_readiness_report(report_path or args.summarize_report)
        summary = summarize_report(report)
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))  # noqa: T201
        elif args.markdown:
            print(render_saved_report_markdown(report))  # noqa: T201
        elif args.print_close_comment:
            print(saved_report_close_comment(report))  # noqa: T201
        else:
            print(render_summary_human(summary))  # noqa: T201
        return 0 if summary.get("accepted") is True else 1
    if args.execute_workflow:
        commit_message, commit_source = _selected_commit_message(args, root)
        report = run_issue_workflow_execution(
            repo_root=root,
            issue=args.issue,
            commit_message=commit_message,
            commit_message_source=commit_source,
            commit_paths=args.commit_paths,
            required_branch=args.required_branch,
            expected_upstream=args.expected_upstream,
            pre_mutation_gates=args.pre_mutation_gates,
            release_preflight=args.release_preflight,
            artifact_roots=args.artifact_roots,
            report_json=args.report_json,
            report_markdown=args.report_markdown,
            runner=runner,
            progress_stream=None if args.quiet_progress else sys.stderr,
            **_acceptance_kwargs(args),
        )
        summary = summarize_issue_workflow_report(report)
        if args.full_json:
            print(json.dumps(report, indent=2, sort_keys=True))  # noqa: T201
        elif args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))  # noqa: T201
        elif args.markdown:
            print(render_issue_workflow_markdown(report))  # noqa: T201
        else:
            print(render_issue_workflow_summary_human(summary))  # noqa: T201
        return (
            0
            if _mapping(report.get("readiness")).get("state") == "ready"
            else 1
        )
    if args.workflow:
        return _run_guided_workflow(args, repo_root=root, runner=runner)
    if args.commit_readiness or args.push_readiness:
        commit_message, commit_source = _selected_commit_message(args, root)
        report = build_commit_readiness_report(
            repo_root=root,
            issue=args.issue,
            mode="push" if args.push_readiness else "commit",
            commit_message=commit_message,
            commit_message_source=commit_source,
            expected_paths=args.commit_paths,
            required_branch=args.required_branch,
            expected_upstream=args.expected_upstream,
            runner=runner,
        )
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True))  # noqa: T201
        else:
            print(render_commit_readiness_human(report))  # noqa: T201
        return (
            0
            if _mapping(report.get("readiness")).get("state") == "ready"
            else 1
        )

    report = build_readiness_report(
        repo_root=root,
        issue=args.issue,
        run_gates=False if args.precheck else args.run_gates,
        release_preflight=False if args.precheck else args.release_preflight,
        artifact_roots=args.artifact_roots,
        runner=runner,
        **_acceptance_kwargs(args),
    )
    json_path, markdown_path, default_json, default_markdown = (
        _selected_report_paths(args, root)
    )
    report = attach_report_paths(
        report,
        json_path=json_path,
        markdown_path=markdown_path,
        repo_root=root,
        default_json=default_json,
        default_markdown=default_markdown,
        runner=runner,
    )
    if args.close_issue:
        report = attach_issue_close_action(
            report,
            repo_root=root,
            runner=runner,
        )
    _write_reports(report, json_path=json_path, markdown_path=markdown_path)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))  # noqa: T201
    elif args.markdown:
        print(render_markdown(report))  # noqa: T201
    elif args.print_close_comment:
        print(str(report.get("close_comment", "")))  # noqa: T201
    elif args.issue_audit:
        print(render_issue_audit_human(report))  # noqa: T201
    elif args.precheck:
        print(render_precheck_human(report))  # noqa: T201
    else:
        print(render_human(report))  # noqa: T201
    if args.issue_audit:
        issue = _mapping(report.get("issue"))
        return 0 if issue.get("state") != "unavailable" else 1
    if args.precheck:
        return (
            0 if _mapping(report.get("precheck")).get("state") == "ready" else 1
        )
    if args.close_issue:
        issue_close = _mapping(report.get("issue_close"))
        return (
            0 if issue_close.get("state") in {"closed", "already-closed"} else 1
        )
    return 0 if _mapping(report.get("readiness")).get("state") == "ready" else 1


def _run_guided_workflow(
    args: argparse.Namespace,
    *,
    repo_root: Path,
    runner: CommandRunner | None,
) -> int:
    """Run precheck-first guided issue closure workflow."""
    report, json_path, markdown_path = build_guided_workflow_report(
        repo_root=repo_root,
        issue=args.issue,
        release_preflight=args.release_preflight,
        artifact_roots=args.artifact_roots,
        required_branch=args.required_branch,
        close_issue=args.close_issue,
        report_json=args.report_json,
        report_markdown=args.report_markdown,
        write_reports=True,
        runner=runner,
        **_acceptance_kwargs(args),
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))  # noqa: T201
    elif args.markdown:
        print(render_markdown(report))  # noqa: T201
    elif args.print_close_comment:
        print(str(report.get("close_comment", "")))  # noqa: T201
    else:
        print(render_human(report))  # noqa: T201
    if args.close_issue:
        issue_close = _mapping(report.get("issue_close"))
        return (
            0 if issue_close.get("state") in {"closed", "already-closed"} else 1
        )
    return 0 if _mapping(report.get("workflow")).get("state") == "ready" else 1


def build_guided_workflow_report(
    *,
    repo_root: Path,
    issue: int,
    release_preflight: bool,
    artifact_roots: Sequence[Path] | None,
    required_branch: str,
    close_issue: bool,
    report_json: Path | None = None,
    report_markdown: Path | None = None,
    write_reports: bool = True,
    runner: CommandRunner | None = None,
    progress: WorkflowProgressReporter | None = None,
    acceptance_statuses: Mapping[str, str] | None = None,
    acceptance_files: Mapping[str, Sequence[str]] | None = None,
    acceptance_tests: Mapping[str, Sequence[str]] | None = None,
    acceptance_reports: Mapping[str, Sequence[str]] | None = None,
    acceptance_notes: Mapping[str, Sequence[str]] | None = None,
    acceptance_missing_ok: bool = False,
    acceptance_override_reason: str = "",
) -> tuple[dict[str, Any], Path | None, Path | None]:
    """Build and optionally close one precheck-first closure workflow report."""
    json_path, markdown_path, default_json, default_markdown = (
        _selected_guided_report_paths(
            repo_root=repo_root,
            issue=issue,
            report_json=report_json,
            report_markdown=report_markdown,
            write_reports=write_reports,
        )
    )
    report = build_readiness_report(
        repo_root=repo_root,
        issue=issue,
        run_gates=False,
        release_preflight=False,
        artifact_roots=artifact_roots,
        runner=runner,
        acceptance_statuses=acceptance_statuses,
        acceptance_files=acceptance_files,
        acceptance_tests=acceptance_tests,
        acceptance_reports=acceptance_reports,
        acceptance_notes=acceptance_notes,
        acceptance_missing_ok=acceptance_missing_ok,
        acceptance_override_reason=acceptance_override_reason,
    )
    report = attach_report_paths(
        report,
        json_path=json_path,
        markdown_path=markdown_path,
        repo_root=repo_root,
        default_json=default_json,
        default_markdown=default_markdown,
        runner=runner,
    )
    report = attach_workflow(report, required_branch=required_branch)
    workflow = _mapping(report.get("workflow"))
    if workflow.get("state") == "ready-to-run-gates":
        if progress is not None:
            progress.start(
                "closure-gates",
                message="Running closure readiness gates",
                command_label="closure gates",
            )
        report = build_readiness_report(
            repo_root=repo_root,
            issue=issue,
            run_gates=True,
            release_preflight=release_preflight,
            artifact_roots=artifact_roots,
            runner=runner,
            acceptance_statuses=acceptance_statuses,
            acceptance_files=acceptance_files,
            acceptance_tests=acceptance_tests,
            acceptance_reports=acceptance_reports,
            acceptance_notes=acceptance_notes,
            acceptance_missing_ok=acceptance_missing_ok,
            acceptance_override_reason=acceptance_override_reason,
        )
        report = attach_report_paths(
            report,
            json_path=json_path,
            markdown_path=markdown_path,
            repo_root=repo_root,
            default_json=default_json,
            default_markdown=default_markdown,
            runner=runner,
        )
        report = attach_workflow(report, required_branch=required_branch)
        if progress is not None:
            readiness_state = str(
                _mapping(report.get("readiness")).get("state", "unknown")
            )
            progress.finish(
                "closure-gates",
                status=(
                    "completed" if readiness_state == "ready" else "blocked"
                ),
                message=f"Closure gates {readiness_state}",
                command_label="closure gates",
            )
    elif progress is not None:
        progress.skip(
            "closure-gates",
            message=f"Workflow state is {workflow.get('state', 'unknown')}",
        )
    if close_issue:
        if progress is not None:
            progress.start(
                "issue-close",
                message="Closing GitHub issue",
                command_label=f"gh issue close {issue}",
            )
        report = attach_issue_close_action(
            report,
            repo_root=repo_root,
            runner=runner,
        )
        if progress is not None:
            issue_close_state = str(
                _mapping(report.get("issue_close")).get("state", "unknown")
            )
            progress.finish(
                "issue-close",
                status=(
                    "completed"
                    if issue_close_state in {"closed", "already-closed"}
                    else "failed"
                ),
                message=f"Issue close {issue_close_state}",
                command_label=f"gh issue close {issue}",
            )
    _write_reports(report, json_path=json_path, markdown_path=markdown_path)
    return report, json_path, markdown_path


def run_issue_workflow_execution(
    *,
    repo_root: Path = PROJECT_ROOT,
    issue: int,
    commit_message: str,
    commit_message_source: str,
    commit_paths: Sequence[Path],
    required_branch: str = DEFAULT_REQUIRED_BRANCH,
    expected_upstream: str = DEFAULT_EXPECTED_UPSTREAM,
    pre_mutation_gates: bool = False,
    release_preflight: bool = False,
    artifact_roots: Sequence[Path] | None = None,
    report_json: Path | None = None,
    report_markdown: Path | None = None,
    runner: CommandRunner | None = None,
    progress_stream: TextIO | None = None,
    now: datetime | None = None,
    acceptance_statuses: Mapping[str, str] | None = None,
    acceptance_files: Mapping[str, Sequence[str]] | None = None,
    acceptance_tests: Mapping[str, Sequence[str]] | None = None,
    acceptance_reports: Mapping[str, Sequence[str]] | None = None,
    acceptance_notes: Mapping[str, Sequence[str]] | None = None,
    acceptance_missing_ok: bool = False,
    acceptance_override_reason: str = "",
) -> dict[str, Any]:
    """Execute the audited issue workflow after report-only readiness passes."""
    command_runner = runner or _run_command
    root = repo_root.expanduser().resolve(strict=False)
    generated_at = now or datetime.now(timezone.utc)
    progress = WorkflowProgressReporter(stream=progress_stream)
    progress.start(
        "report-paths",
        message="Checking execution report paths",
    )
    json_path, markdown_path, default_json, default_markdown = (
        _selected_execution_report_paths(
            repo_root=root,
            issue=issue,
            report_json=report_json,
            report_markdown=report_markdown,
        )
    )
    report_paths = _execution_report_path_payloads(
        json_path=json_path,
        markdown_path=markdown_path,
        repo_root=root,
        default_json=default_json,
        default_markdown=default_markdown,
        runner=command_runner,
    )
    blockers = _report_path_blockers(report_paths)
    warnings = _report_path_warnings(report_paths)
    log_dir = _default_execution_log_dir(root, issue)
    log_payload = {
        "directory": str(publish_safe_path(str(log_dir.resolve(strict=False)))),
        "state": "ready" if not blockers else "blocked",
        "reason": (
            ""
            if not blockers
            else "execution report paths must be ignored before logs are written"
        ),
    }
    report_path_message = (
        "Execution report paths are ready"
        if not blockers
        else "Execution report paths are blocked"
    )
    progress.finish(
        "report-paths",
        status="completed" if not blockers else "blocked",
        message=report_path_message,
        report_path=str(publish_safe_path(str(json_path))),
    )
    if blockers:
        progress.start(
            "report-writing",
            message="Writing blocked workflow evidence",
            report_path=str(publish_safe_path(str(json_path))),
        )
        progress.finish(
            "report-writing",
            status="completed",
            message="Blocked workflow evidence written",
            report_path=str(publish_safe_path(str(json_path))),
        )
        report = _issue_workflow_execution_report(
            generated_at=generated_at,
            issue=issue,
            required_branch=required_branch,
            expected_upstream=expected_upstream,
            release_preflight=release_preflight,
            report_paths=report_paths,
            logs=log_payload,
            commands=(),
            commit_readiness={},
            pre_mutation_gate_report=_pre_mutation_gates_not_run(
                enabled=pre_mutation_gates,
            ),
            push_readiness={},
            closure_report={},
            closure_summary={},
            process_before={},
            process_after={},
            final_readback={},
            state="blocked",
            blockers=blockers,
            warnings=warnings,
            workflow_progress=progress.payload(),
        )
        _write_execution_reports(
            report,
            json_path=json_path,
            markdown_path=markdown_path,
        )
        return report

    logger = WorkflowExecutionLogger(
        repo_root=root,
        log_dir=log_dir,
        runner=command_runner,
    )
    progress.start(
        "initial-readiness",
        message="Checking process state and commit readiness",
    )
    process_before = collect_process_summary(root, runner=logger)
    if process_before.get("state") == "dirty":
        blockers.append("lingering-processes-before")
    if not commit_paths:
        blockers.append("commit-paths-missing")
    commit_report = build_commit_readiness_report(
        repo_root=root,
        issue=issue,
        mode="commit",
        commit_message=commit_message,
        commit_message_source=commit_message_source,
        expected_paths=commit_paths,
        required_branch=required_branch,
        expected_upstream=expected_upstream,
        runner=logger,
        now=generated_at,
    )
    _merge_readiness_findings(
        blockers,
        warnings,
        _mapping(commit_report.get("readiness")),
    )
    progress.finish(
        "initial-readiness",
        status="completed" if not blockers else "blocked",
        message=(
            "Initial readiness is ready"
            if not blockers
            else "Initial readiness is blocked"
        ),
    )

    push_report: dict[str, Any] = {}
    closure_report: dict[str, Any] = {}
    closure_summary: dict[str, Any] = {}
    pre_mutation_gate_report = _pre_mutation_gates_not_run(
        enabled=pre_mutation_gates,
    )
    stage_paths = _execution_stage_paths(commit_report)
    execution_acceptance_files = _merge_acceptance_value_maps(
        acceptance_files,
        _all_acceptance_map(stage_paths),
    )
    execution_acceptance_tests = _merge_acceptance_value_maps(
        acceptance_tests,
        _all_acceptance_map(_acceptance_tests_from_paths(stage_paths)),
    )
    execution_acceptance_reports = _merge_acceptance_value_maps(
        acceptance_reports,
        _all_acceptance_map(_acceptance_report_paths(report_paths)),
    )
    state = "blocked" if blockers else "running"

    if state == "running" and pre_mutation_gates:
        progress.start(
            "pre-mutation-gates",
            message="Running gates before git mutation",
            command_label="closure gates",
        )
        pre_mutation_gate_report = run_pre_mutation_gates(
            root,
            changed_paths=tuple(
                str(path)
                for path in list(
                    _mapping(commit_report.get("changes")).get(
                        "changed_paths",
                        [],
                    )
                    or []
                )
            ),
            runner=logger,
        )
        pre_mutation_state = str(
            pre_mutation_gate_report.get("state", "unknown")
        )
        progress.finish(
            "pre-mutation-gates",
            status="completed" if pre_mutation_state == "pass" else "blocked",
            message=f"Pre-mutation gates {pre_mutation_state}",
            command_label="closure gates",
        )
        if pre_mutation_state != "pass":
            for blocker in list(
                pre_mutation_gate_report.get("blocking_checks", []) or []
            ):
                text = str(blocker)
                if text not in blockers:
                    blockers.append(text)
            state = "blocked"
    elif state == "running":
        progress.skip(
            "pre-mutation-gates",
            message="Pre-mutation gates were not requested",
        )

    if state == "running":
        progress.start(
            "staging",
            message="Staging declared workflow paths",
            command_label="git add",
        )
        stage_result = logger.run(
            ("git", "add", "--", *stage_paths),
            root,
            name="git-add",
        )
        progress.finish(
            "staging",
            status="completed" if stage_result.returncode == 0 else "failed",
            message="Staging completed"
            if stage_result.returncode == 0
            else "Staging failed",
            command_label="git add",
            log_path=str(logger.records[-1].get("log_path", "")),
        )
        if stage_result.returncode != 0:
            blockers.append("command:git-add")
            state = "failed"
    if state == "running":
        progress.start(
            "commit",
            message="Creating workflow commit",
            command_label="git commit",
        )
        commit_result = logger.run(
            ("git", "commit", "-m", commit_message),
            root,
            name="git-commit",
        )
        progress.finish(
            "commit",
            status="completed" if commit_result.returncode == 0 else "failed",
            message="Commit completed"
            if commit_result.returncode == 0
            else "Commit failed",
            command_label="git commit",
            log_path=str(logger.records[-1].get("log_path", "")),
        )
        if commit_result.returncode != 0:
            blockers.append("command:git-commit")
            state = "failed"
    if state == "running":
        progress.start(
            "push-readiness",
            message="Checking push readiness",
        )
        push_report = build_commit_readiness_report(
            repo_root=root,
            issue=issue,
            mode="push",
            required_branch=required_branch,
            expected_upstream=expected_upstream,
            runner=logger,
            now=generated_at,
        )
        push_readiness = _mapping(push_report.get("readiness"))
        progress.finish(
            "push-readiness",
            status=(
                "completed"
                if push_readiness.get("state") == "ready"
                else "blocked"
            ),
            message=f"Push readiness {push_readiness.get('state', 'unknown')}",
        )
        if push_readiness.get("state") != "ready":
            _merge_readiness_findings(blockers, warnings, push_readiness)
            state = "blocked"
    if state == "running":
        progress.start(
            "push",
            message="Pushing workflow commit",
            command_label="git push",
        )
        push_result = logger.run(
            _push_command(expected_upstream, required_branch),
            root,
            name="git-push",
        )
        progress.finish(
            "push",
            status="completed" if push_result.returncode == 0 else "failed",
            message="Push completed"
            if push_result.returncode == 0
            else "Push failed",
            command_label="git push",
            log_path=str(logger.records[-1].get("log_path", "")),
        )
        if push_result.returncode != 0:
            blockers.append("command:git-push")
            state = "failed"
    if state == "running":
        closure_report, _, _ = build_guided_workflow_report(
            repo_root=root,
            issue=issue,
            release_preflight=release_preflight,
            artifact_roots=artifact_roots,
            required_branch=required_branch,
            close_issue=True,
            write_reports=True,
            runner=logger,
            progress=progress,
            acceptance_statuses=acceptance_statuses,
            acceptance_files=execution_acceptance_files,
            acceptance_tests=execution_acceptance_tests,
            acceptance_reports=execution_acceptance_reports,
            acceptance_notes=acceptance_notes,
            acceptance_missing_ok=acceptance_missing_ok,
            acceptance_override_reason=acceptance_override_reason,
        )
        closure_summary = summarize_readiness_report(closure_report)
        if closure_summary.get("accepted") is not True:
            blockers.append("closure-workflow")
            state = "failed"
    elif state in {"blocked", "failed"}:
        progress.skip(
            "closure-gates",
            message=f"Skipped because workflow state is {state}",
        )
        progress.skip(
            "issue-close",
            message=f"Skipped because workflow state is {state}",
        )
    progress.start(
        "final-readback",
        message="Reading final repository, issue, and commit state",
    )
    final_readback = {
        "repo": collect_git_state(root, runner=logger),
        "issue": collect_issue_state(root, issue, runner=logger),
        "commit": _last_commit_payload(root, runner=logger),
    }
    progress.finish(
        "final-readback",
        status="completed",
        message="Final readback completed",
    )
    process_after = collect_process_summary(root, runner=logger)
    if process_after.get("state") == "dirty":
        blockers.append("lingering-processes-after")
        state = "failed" if state != "blocked" else state
    if state == "running":
        state = "ready"

    log_payload["command_count"] = len(logger.records)
    progress.start(
        "report-writing",
        message="Writing workflow evidence reports",
        report_path=str(publish_safe_path(str(json_path))),
    )
    progress.finish(
        "report-writing",
        status="completed",
        message="Workflow evidence reports written",
        report_path=str(publish_safe_path(str(json_path))),
    )
    report = _issue_workflow_execution_report(
        generated_at=generated_at,
        issue=issue,
        required_branch=required_branch,
        expected_upstream=expected_upstream,
        release_preflight=release_preflight,
        report_paths=report_paths,
        logs=log_payload,
        commands=tuple(logger.records),
        commit_readiness=commit_report,
        pre_mutation_gate_report=pre_mutation_gate_report,
        push_readiness=push_report,
        closure_report=closure_report,
        closure_summary=closure_summary,
        process_before=process_before,
        process_after=process_after,
        final_readback=final_readback,
        state=state,
        blockers=blockers,
        warnings=warnings,
        workflow_progress=progress.payload(),
    )
    _write_execution_reports(
        report,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    return report


def _issue_workflow_execution_report(
    *,
    generated_at: datetime,
    issue: int,
    required_branch: str,
    expected_upstream: str,
    release_preflight: bool,
    report_paths: Mapping[str, Any],
    logs: Mapping[str, Any],
    commands: Sequence[Mapping[str, Any]],
    commit_readiness: Mapping[str, Any],
    pre_mutation_gate_report: Mapping[str, Any],
    push_readiness: Mapping[str, Any],
    closure_report: Mapping[str, Any],
    closure_summary: Mapping[str, Any],
    process_before: Mapping[str, Any],
    process_after: Mapping[str, Any],
    final_readback: Mapping[str, Any],
    state: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
    workflow_progress: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the publish-safe executable workflow evidence payload."""
    report: dict[str, Any] = {
        "schema_version": ISSUE_WORKFLOW_SCHEMA_VERSION,
        "operation": "issue-workflow-execution",
        "generated_at_utc": generated_at.astimezone(timezone.utc).isoformat(),
        "issue_number": issue,
        "required_branch": required_branch,
        "expected_upstream": expected_upstream,
        "release_preflight": {
            "requested": bool(release_preflight),
            "policy": (
                "explicit"
                if release_preflight
                else "not-run-for-non-release-work"
            ),
        },
        "readiness": {
            "state": state,
            "blocking_checks": list(
                dict.fromkeys(str(item) for item in blockers)
            ),
            "warnings": list(dict.fromkeys(str(item) for item in warnings)),
        },
        "report_paths": dict(report_paths),
        "logs": dict(logs),
        "processes_before": dict(process_before),
        "processes_after": dict(process_after),
        "commit_readiness": dict(commit_readiness),
        "pre_mutation_gates": dict(pre_mutation_gate_report),
        "push_readiness": dict(push_readiness),
        "commands": [dict(command) for command in commands],
        "acceptance_coverage": dict(
            _mapping(closure_report.get("acceptance_coverage"))
        ),
        "closure_summary": dict(closure_summary),
        "closure_report": dict(closure_report),
        "final_readback": dict(final_readback),
        "workflow_progress": dict(workflow_progress or {}),
    }
    safe = publish_safe_json_value(report)
    if not isinstance(safe, dict):
        raise TypeError("issue workflow execution report must be a JSON object")
    return dict(safe)


def _merge_readiness_findings(
    blockers: list[str],
    warnings: list[str],
    readiness: Mapping[str, Any],
) -> None:
    for blocker in list(readiness.get("blocking_checks", []) or []):
        text = str(blocker)
        if text not in blockers:
            blockers.append(text)
    for warning in list(readiness.get("warnings", []) or []):
        text = str(warning)
        if text not in warnings:
            warnings.append(text)


def run_pre_mutation_gates(
    repo_root: Path,
    *,
    changed_paths: Sequence[str],
    runner: CommandRunner,
) -> dict[str, Any]:
    """Run closure gates before git mutation and report unsafe drift."""
    before = _path_fingerprints(repo_root, changed_paths)
    gates = collect_gate_summary(repo_root, run_gates=True, runner=runner)
    after = _path_fingerprints(repo_root, changed_paths)
    gate_changed = _gate_changed_paths(gates)
    fingerprint_changed = _fingerprint_changed_paths(before, after)
    changed_after = _unique_sorted((*gate_changed, *fingerprint_changed))
    failed_gates = [
        str(_mapping(result).get("name", "unknown"))
        for result in list(gates.get("results", []) or [])
        if _mapping(result).get("status") != "pass"
    ]
    blockers: list[str] = []
    if gates.get("state") != "pass":
        blockers.append("pre-mutation-gates-failed")
        blockers.extend(f"pre-mutation-gate:{name}" for name in failed_gates)
    if changed_after:
        blockers.append("pre-mutation-gates-changed-files")
    return {
        "enabled": True,
        "state": "pass" if not blockers else "blocked",
        "blocking_checks": list(dict.fromkeys(blockers)),
        "changed_paths_after": list(changed_after),
        "gate_changed_paths": list(gate_changed),
        "fingerprint_changed_paths": list(fingerprint_changed),
        "gates": gates,
        "results": list(gates.get("results", []) or []),
    }


def _pre_mutation_gates_not_run(*, enabled: bool) -> dict[str, Any]:
    reason = (
        "blocked before pre-mutation gates could run"
        if enabled
        else "run --execute-workflow with --pre-mutation-gates to enable"
    )
    return {
        "enabled": enabled,
        "state": "not-run",
        "reason": reason,
        "blocking_checks": [],
        "changed_paths_after": [],
        "gate_changed_paths": [],
        "fingerprint_changed_paths": [],
        "gates": {
            "state": "not-run",
            "reason": reason,
            "required": [gate.display for gate in GATE_SPECS],
            "results": [],
        },
        "results": [],
    }


def _gate_changed_paths(gates: Mapping[str, Any]) -> tuple[str, ...]:
    values: list[str] = []
    for result in list(gates.get("results", []) or []):
        result_map = _mapping(result)
        values.extend(
            str(path)
            for path in list(result_map.get("changed_paths_after", []) or [])
        )
    return _unique_sorted(values)


def _path_fingerprints(
    repo_root: Path,
    paths: Sequence[str],
) -> dict[str, Any]:
    fingerprints: dict[str, Any] = {}
    for path in _unique_sorted(str(item) for item in paths):
        if not path:
            continue
        relative = Path(path)
        resolved = (
            relative.expanduser()
            if relative.is_absolute()
            else repo_root / relative
        )
        fingerprints[str(publish_safe_path(path))] = _path_fingerprint(
            resolved,
        )
    return fingerprints


def _path_fingerprint(path: Path) -> dict[str, Any]:
    try:
        if path.is_symlink():
            return {
                "state": "symlink",
                "target": str(publish_safe_path(str(path.readlink()))),
            }
        if path.is_file():
            stat = path.stat()
            return {
                "state": "file",
                "size_bytes": stat.st_size,
                "sha256": _file_sha256(path),
            }
        if path.is_dir():
            return {"state": "directory"}
        if path.exists():
            return {"state": "other"}
        return {"state": "missing"}
    except OSError as exc:
        return {
            "state": "unavailable",
            "reason": exc.__class__.__name__,
        }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fingerprint_changed_paths(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> tuple[str, ...]:
    return _unique_sorted(
        path
        for path in set(before).union(after)
        if _mapping(before.get(path)) != _mapping(after.get(path))
    )


def _execution_stage_paths(report: Mapping[str, Any]) -> tuple[str, ...]:
    scope = _mapping(report.get("scope"))
    paths = tuple(str(path) for path in scope.get("declared_paths", []) or [])
    if paths:
        return paths
    changes = _mapping(report.get("changes"))
    return tuple(str(path) for path in changes.get("changed_paths", []) or [])


def _push_command(
    expected_upstream: str, required_branch: str
) -> tuple[str, ...]:
    if "/" in expected_upstream:
        remote, branch = expected_upstream.split("/", 1)
        return ("git", "push", remote, branch)
    return ("git", "push", "origin", required_branch)


def _last_commit_payload(
    repo_root: Path,
    *,
    runner: CommandRunner,
) -> dict[str, Any]:
    result = _run_git(
        repo_root, ("log", "-1", "--oneline", "--decorate"), runner
    )
    return {
        "state": "available" if result.returncode == 0 else "unavailable",
        "returncode": result.returncode,
        "summary": _tail_text(result.stdout, line_limit=1),
        "stderr_tail": _tail_text(result.stderr),
    }


def render_issue_workflow_human(report: Mapping[str, Any]) -> str:
    """Render a compact executable workflow console summary."""
    return render_issue_workflow_summary_human(
        summarize_issue_workflow_report(report)
    )


def render_issue_workflow_markdown(report: Mapping[str, Any]) -> str:
    """Render publish-safe Markdown for an executable issue workflow."""
    readiness = _mapping(report.get("readiness"))
    final = _mapping(report.get("final_readback"))
    final_repo = _mapping(final.get("repo"))
    final_issue = _mapping(final.get("issue"))
    closure_summary = _mapping(report.get("closure_summary"))
    closure_gates = _mapping(
        _mapping(report.get("closure_report")).get("gates")
    )
    pre_mutation = _mapping(report.get("pre_mutation_gates"))
    pre_mutation_gates = _mapping(pre_mutation.get("gates"))
    acceptance = _mapping(report.get("acceptance_coverage"))
    lines = [
        "# Issue Workflow Execution",
        "",
        f"- State: **{readiness.get('state', 'unknown')}**",
        f"- Issue: {_issue_label(final_issue)}",
        f"- Branch: `{final_repo.get('branch', 'unknown')}`",
        f"- Upstream: `{final_repo.get('upstream', '')}` "
        f"(ahead {final_repo.get('ahead', 0)}, behind {final_repo.get('behind', 0)})",
        f"- Expected upstream: `{report.get('expected_upstream', '')}`",
        f"- Release preflight policy: "
        f"{_mapping(report.get('release_preflight')).get('policy', '')}",
        "",
        "## Blocking Checks",
        "",
    ]
    blockers = list(readiness.get("blocking_checks", []) or [])
    if blockers:
        lines.extend(f"- `{blocker}`" for blocker in blockers)
    else:
        lines.append("- None")
    lines.extend(_render_acceptance_markdown(acceptance))
    lines.extend(
        [
            "",
            "## Pre-Mutation Gates",
            "",
            f"- State: {pre_mutation.get('state', 'not-run')}",
            f"- Enabled: {_yes_no(pre_mutation.get('enabled'))}",
            f"- Gate labels: {_gate_labels(pre_mutation_gates)}",
            "",
        ]
    )
    changed_paths = list(pre_mutation.get("changed_paths_after", []) or [])
    if changed_paths:
        lines.append("- Changed paths after gates:")
        lines.extend(f"  - `{path}`" for path in changed_paths)
        lines.append("")
    lines.extend(
        [
            "## Commands",
            "",
            "| Step | Status | Return code | Log |",
            "| --- | --- | ---: | --- |",
        ]
    )
    for command in list(report.get("commands", []) or []):
        payload = _mapping(command)
        lines.append(
            f"| `{payload.get('name', '')}` | {payload.get('status', '')} | "
            f"{payload.get('returncode', '')} | `{payload.get('log_path', '')}` |"
        )
    lines.extend(
        [
            "",
            "## Closure Gates",
            "",
            f"- Closure accepted: {_yes_no(closure_summary.get('accepted'))}",
            f"- Gate labels: {_gate_labels(closure_gates)}",
            f"- Issue close: "
            f"{_mapping(closure_summary.get('issue_close')).get('state', 'not-run')}",
            "",
            "## Process Health",
            "",
            f"- Before: {_mapping(report.get('processes_before')).get('state', 'unknown')}",
            f"- After: {_mapping(report.get('processes_after')).get('state', 'unknown')}",
            "",
        ]
    )
    return "\n".join(lines)


def _readiness_state(report: Mapping[str, Any]) -> str:
    if not report:
        return "not-run"
    return str(_mapping(report.get("readiness")).get("state", "unknown"))


def _run_gate(
    repo_root: Path,
    gate: GateSpec,
    *,
    runner: CommandRunner,
) -> dict[str, Any]:
    before = set(_git_changed_paths(repo_root, runner))
    result = runner(gate.command, repo_root)
    after = set(_git_changed_paths(repo_root, runner))
    changed_after = sorted(after - before)
    if (
        gate.name == "pre-commit"
        and not changed_after
        and result.returncode != 0
    ):
        changed_after = sorted(after)
    return {
        "name": gate.name,
        "command": gate.display,
        "status": "pass" if result.returncode == 0 else "fail",
        "returncode": result.returncode,
        "stdout_tail": _tail_text(result.stdout),
        "stderr_tail": _tail_text(result.stderr),
        "changed_paths_after": changed_after,
    }


def _run_command(
    command: Sequence[str],
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        cwd=cwd,
        capture_output=True,
        check=False,
        text=True,
    )


def _run_git(
    repo_root: Path,
    args: Sequence[str],
    runner: CommandRunner,
) -> subprocess.CompletedProcess[str]:
    return runner(("git", *args), repo_root)


def _git_stdout(
    repo_root: Path,
    args: Sequence[str],
    runner: CommandRunner,
) -> str:
    result = _run_git(repo_root, args, runner)
    return result.stdout.strip() if result.returncode == 0 else ""


def _upstream_counts(
    repo_root: Path,
    has_upstream: bool,
    runner: CommandRunner,
) -> dict[str, int]:
    if not has_upstream:
        return {"ahead": 0, "behind": 0}
    result = _run_git(
        repo_root,
        ("rev-list", "--left-right", "--count", "HEAD...@{u}"),
        runner,
    )
    if result.returncode != 0:
        return {"ahead": 0, "behind": 0}
    parts = result.stdout.split()
    if len(parts) < 2:
        return {"ahead": 0, "behind": 0}
    return {"ahead": _int(parts[0]), "behind": _int(parts[1])}


def _git_changed_paths(
    repo_root: Path,
    runner: CommandRunner,
) -> tuple[str, ...]:
    result = _run_git(
        repo_root,
        ("status", "--porcelain=v1", "--untracked-files=all"),
        runner,
    )
    if result.returncode != 0:
        return ()
    return _status_paths(tuple(result.stdout.splitlines()))


def _status_paths(status_lines: Sequence[str]) -> tuple[str, ...]:
    paths: list[str] = []
    for line in status_lines:
        if not line.strip():
            continue
        path = line[3:] if len(line) > 3 else line
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(str(publish_safe_path(path.strip())))
    return tuple(sorted(paths))


def _iter_process_observations(
    rows: Iterable[str],
) -> Iterable[ProcessObservation]:
    for row in rows:
        parts = row.strip().split(None, 2)
        if len(parts) < 2:
            continue
        pid_text, command = parts[:2]
        args = parts[2] if len(parts) > 2 else ""
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        category = _process_category(command, args)
        if category is None:
            continue
        yield ProcessObservation(
            pid=pid,
            category=category,
            command=Path(command).name or command,
        )


def _process_category(command: str, args: str) -> str | None:
    blob = f"{command} {args}".lower()
    if "closure_readiness.py" in blob:
        return None
    if "pytest" in blob:
        return "pytest"
    if "pre_commit" in blob or "pre-commit" in blob:
        return "pre-commit"
    if "histdatacom.orchestration.worker" in blob:
        return "temporal-worker"
    if "temporal" in blob and "worker" in blob:
        return "temporal-worker"
    if "temporal" in blob:
        return "temporal-runtime"
    if "histdatacom" in blob:
        return "histdatacom"
    if re.search(r"(^|[/\s])ruff(\s|$)", blob):
        return "ruff"
    if re.search(r"(^|[/\s])mypy(\s|$)", blob):
        return "mypy"
    return None


def _gate_labels(gates: Mapping[str, Any]) -> str:
    results = list(gates.get("results", []) or [])
    if not results:
        return str(gates.get("state", "unknown"))
    labels = []
    for result in results:
        result_map = _mapping(result)
        labels.append(f"{result_map.get('name')}={result_map.get('status')}")
    return ", ".join(labels)


def _issue_label(issue: Mapping[str, Any]) -> str:
    if issue.get("requested") and issue.get("number"):
        return f"#{issue.get('number')} {issue.get('state', 'unknown')}"
    return str(issue.get("state", "not-requested"))


def _issue_is_closed(issue: Mapping[str, Any]) -> bool:
    return str(issue.get("state", "")).upper() == "CLOSED"


def _tail_text(
    value: str,
    *,
    line_limit: int = DEFAULT_TAIL_LINE_LIMIT,
    char_limit: int = DEFAULT_TAIL_CHAR_LIMIT,
) -> str:
    lines = value.splitlines()
    tail = "\n".join(lines[-line_limit:])
    if len(tail) > char_limit:
        tail = tail[-char_limit:]
    safe = publish_safe_json_value(tail)
    return str(safe)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _int(value: object) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _yes_no(value: object) -> str:
    return "yes" if bool(value) else "no"


def _selected_commit_message(
    args: argparse.Namespace,
    repo_root: Path,
) -> tuple[str, str]:
    if args.commit_message_file is None:
        return str(args.commit_message or ""), "argument"
    path = _output_path(args.commit_message_file, repo_root)
    if path is None:
        return "", "file"
    try:
        return path.read_text(encoding="utf-8").strip(), "file"
    except OSError as exc:
        return "", f"file-unavailable:{exc.__class__.__name__}"


def _commit_message_payload(
    repo_root: Path,
    *,
    commit_message: str,
    source: str,
    mode: str,
    runner: CommandRunner,
) -> dict[str, Any]:
    if mode == "push":
        return {
            "state": "not-applicable",
            "provided": False,
            "source": source,
        }
    if not commit_message.strip():
        return {
            "state": "missing",
            "provided": False,
            "source": source,
        }
    result = runner(
        (
            sys.executable,
            "-m",
            "commitizen",
            "check",
            "--message",
            commit_message,
        ),
        repo_root,
    )
    return {
        "state": "valid" if result.returncode == 0 else "invalid",
        "provided": True,
        "source": source,
        "message": commit_message,
        "validator": "python -m commitizen check --message <message>",
        "returncode": result.returncode,
        "stdout_tail": _tail_text(result.stdout),
        "stderr_tail": _tail_text(result.stderr),
    }


def _commit_scope_payload(
    changes: Mapping[str, Any],
    *,
    repo_root: Path,
    expected_paths: Sequence[Path],
) -> dict[str, Any]:
    declared = _declared_scope_paths(expected_paths, repo_root=repo_root)
    changed = [
        str(item) for item in list(changes.get("changed_paths", []) or [])
    ]
    if not declared:
        return {
            "state": "not-declared",
            "declared_paths": [],
            "unrelated_paths": [],
            "covered_paths": changed,
        }
    unrelated = [
        path
        for path in changed
        if not any(_path_in_scope(path, scope) for scope in declared)
    ]
    covered = [path for path in changed if path not in unrelated]
    return {
        "state": "clean" if not unrelated else "dirty-unrelated",
        "declared_paths": list(declared),
        "unrelated_paths": unrelated,
        "covered_paths": covered,
    }


def _declared_scope_paths(
    paths: Sequence[Path],
    *,
    repo_root: Path,
) -> tuple[str, ...]:
    normalized: list[str] = []
    for path in paths:
        expanded = path.expanduser()
        if expanded.is_absolute():
            resolved = expanded.resolve(strict=False)
            try:
                normalized.append(resolved.relative_to(repo_root).as_posix())
            except ValueError:
                normalized.append(str(publish_safe_path(str(resolved))))
        else:
            normalized.append(expanded.as_posix())
    return _unique_sorted(item.rstrip("/") for item in normalized if item)


def _path_in_scope(path: str, scope: str) -> bool:
    scope_value = scope.rstrip("/")
    return path == scope_value or path.startswith(f"{scope_value}/")


def _commit_command_plan(report: Mapping[str, Any]) -> list[str]:
    mode = str(report.get("mode", "commit"))
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    branch = str(repo.get("branch", "dev") or "dev")
    commands: list[str] = []
    if mode == "push":
        commands.append(_shell_command(("git", "push", "origin", branch)))
        closure = _closure_command(issue)
        if closure:
            commands.append(closure)
        return commands

    changes = _mapping(report.get("changes"))
    message = _mapping(report.get("commit_message"))
    scope = _mapping(report.get("scope"))
    staged_count = _int(changes.get("staged_count"))
    needs_stage = (
        _int(changes.get("unstaged_count")) > 0
        or _int(changes.get("untracked_count")) > 0
    )
    stage_paths = list(scope.get("declared_paths", []) or [])
    if not stage_paths:
        stage_paths = list(changes.get("changed_paths", []) or [])
    if needs_stage and stage_paths:
        commands.append(_shell_command(("git", "add", "--", *stage_paths)))
    if staged_count > 0 or needs_stage:
        commit_text = str(message.get("message", "") or "<message>")
        commands.append(_shell_command(("git", "commit", "-m", commit_text)))
        commands.append(_shell_command(("git", "push", "origin", branch)))
    closure = _closure_command(issue)
    if closure:
        commands.append(closure)
    return commands


def _closure_command(issue: Mapping[str, Any]) -> str:
    number = _int(issue.get("number"))
    if number <= 0:
        return ""
    return _shell_command(
        (
            sys.executable,
            "scripts/closure_readiness.py",
            "--issue",
            str(number),
            "--workflow",
            "--close-issue",
        )
    )


def _workflow_command_payload(
    command: Sequence[str],
    result: subprocess.CompletedProcess[str],
    *,
    name: str,
    log_path: Path,
    repo_root: Path,
) -> dict[str, Any]:
    try:
        display_path = log_path.resolve(strict=False).relative_to(repo_root)
    except ValueError:
        display_path = publish_safe_path(str(log_path.resolve(strict=False)))
    return {
        "name": name,
        "command": _shell_command(command),
        "status": "pass" if result.returncode == 0 else "fail",
        "returncode": result.returncode,
        "stdout_tail": _tail_text(result.stdout),
        "stderr_tail": _tail_text(result.stderr),
        "log_path": str(publish_safe_path(str(display_path))),
    }


def _workflow_command_name(command: Sequence[str]) -> str:
    args = tuple(str(part) for part in command)
    if args[:3] == ("git", "add", "--"):
        return "git-add"
    if args[:3] == ("git", "commit", "-m"):
        return "git-commit"
    if args[:2] == ("git", "push"):
        return "git-push"
    if args[:3] == ("gh", "issue", "close"):
        return "gh-issue-close"
    if args[:3] == ("gh", "issue", "view"):
        return "gh-issue-view"
    if args[:2] == ("git", "status"):
        return "git-status"
    if args[:2] == ("git", "rev-parse"):
        return "git-rev-parse"
    if args[:2] == ("git", "rev-list"):
        return "git-rev-list"
    if args[:2] == ("git", "log"):
        return "git-log"
    if args[:3] == (sys.executable, "-m", "pytest"):
        return "gate-pytest"
    if args[:3] == (sys.executable, "-m", "pre_commit"):
        return "gate-pre-commit"
    if args[:2] == (sys.executable, "scripts/sync_readme_cli_help.py"):
        return "gate-readme-help-sync"
    if args[:3] == (sys.executable, "-m", "histdatacom"):
        return "gate-main-help-smoke"
    if args[:2] == ("git", "diff"):
        return "gate-git-diff-check"
    if args[:4] == (sys.executable, "-m", "commitizen", "check"):
        return "commitizen-check"
    if args[:2] == ("ps", "-axo"):
        return "process-check"
    if args[:2] == ("git", "check-ignore"):
        return "git-check-ignore"
    return _slug(args[0] if args else "command")


def _slug(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or "command"


def _shell_command(parts: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in parts)


def _status_entry(line: str) -> dict[str, Any] | None:
    if not line.strip():
        return None
    if len(line) < 3:
        return None
    x_status = line[0]
    y_status = line[1]
    path = line[3:].strip()
    if " -> " in path:
        path = path.split(" -> ", 1)[1]
    path = str(publish_safe_path(path))
    untracked = line.startswith("??")
    staged = (not untracked) and x_status not in {" ", "?"}
    unstaged = (not untracked) and y_status != " "
    return {
        "status": line[:2],
        "path": path,
        "staged": staged,
        "unstaged": unstaged,
        "untracked": untracked,
    }


def _unique_sorted(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(dict.fromkeys(value for value in values if value)))


def _apply_report_path_readiness(report: Mapping[str, Any]) -> dict[str, Any]:
    """Return report with report path blockers applied to readiness fields."""
    report_paths = _mapping(report.get("report_paths"))
    blockers = _report_path_blockers(report_paths)
    warnings = _report_path_warnings(report_paths)
    if not blockers and not warnings:
        return dict(report)
    updated: dict[str, Any] = dict(report)
    for field in ("precheck", "readiness"):
        section = dict(_mapping(updated.get(field)))
        blocking_checks = [
            str(item) for item in list(section.get("blocking_checks", []) or [])
        ]
        section_warnings = [
            str(item) for item in list(section.get("warnings", []) or [])
        ]
        for blocker in blockers:
            if blocker not in blocking_checks:
                blocking_checks.append(blocker)
        for warning in warnings:
            if warning not in section_warnings:
                section_warnings.append(warning)
        section["blocking_checks"] = blocking_checks
        section["warnings"] = section_warnings
        section["state"] = "ready" if not blocking_checks else "blocked"
        if field == "precheck":
            section["ready_to_run_gates"] = section["state"] == "ready"
        updated[field] = section
    return updated


def _report_path_blockers(report_paths: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    for kind, payload in report_paths.items():
        payload_map = _mapping(payload)
        if not payload_map.get("closure_blocking"):
            continue
        state = str(payload_map.get("gitignore_state", "unknown"))
        if state == "not-ignored":
            blockers.append(f"report-path-not-ignored:{kind}")
        else:
            blockers.append(f"report-path-ignore-unverified:{kind}")
    return blockers


def _report_path_warnings(report_paths: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    for kind, payload in report_paths.items():
        payload_map = _mapping(payload)
        if payload_map.get("closure_blocking"):
            continue
        if payload_map.get("repository_scope") != "inside-repo":
            continue
        if payload_map.get("gitignored") is True:
            continue
        warnings.append(f"report-path-may-dirty-worktree:{kind}")
    return warnings


def _report_paths_summary(report_paths: Mapping[str, Any]) -> dict[str, Any]:
    if not report_paths:
        return {
            "state": "not-recorded",
            "blocking_checks": [],
            "warnings": [],
            "outputs": {},
        }
    blockers = _report_path_blockers(report_paths)
    warnings = _report_path_warnings(report_paths)
    outputs = {}
    for kind, payload in report_paths.items():
        payload_map = _mapping(payload)
        outputs[str(kind)] = {
            "path": payload_map.get("path", ""),
            "default": bool(payload_map.get("default")),
            "gitignore_state": payload_map.get("gitignore_state", "unknown"),
            "workspace_effect": payload_map.get(
                "workspace_effect",
                "unknown",
            ),
            "write_allowed": bool(payload_map.get("write_allowed", True)),
            "closure_blocking": bool(
                payload_map.get("closure_blocking", False)
            ),
        }
    state = "ready"
    if blockers:
        state = "blocked"
    elif warnings:
        state = "warning"
    return {
        "state": state,
        "blocking_checks": blockers,
        "warnings": warnings,
        "outputs": outputs,
    }


def _git_ignore_status(
    repo_root: Path,
    relative_path: str,
    runner: CommandRunner,
) -> dict[str, Any]:
    result = _run_git(
        repo_root,
        ("check-ignore", "-q", "--", relative_path),
        runner,
    )
    if result.returncode == 0:
        return {
            "gitignore_state": "ignored",
            "gitignored": True,
        }
    if result.returncode == 1:
        return {
            "gitignore_state": "not-ignored",
            "gitignored": False,
        }
    return {
        "gitignore_state": "unavailable",
        "gitignored": None,
        "gitignore_reason": _tail_text(result.stderr or result.stdout),
    }


def _report_write_allowed(
    report_paths: Mapping[str, Any],
    kind: str,
) -> bool:
    payload = _mapping(report_paths.get(kind))
    return bool(payload.get("write_allowed", True))


def _selected_report_paths(
    args: argparse.Namespace,
    repo_root: Path,
) -> tuple[Path | None, Path | None, bool, bool]:
    default_paths = _default_report_paths(repo_root, args.issue)
    json_path = _output_path(args.report_json, repo_root)
    markdown_path = _output_path(args.report_markdown, repo_root)
    default_json = False
    default_markdown = False
    if args.workflow or args.write_reports:
        if json_path is None:
            json_path = default_paths["json"]
            default_json = True
        if markdown_path is None:
            markdown_path = default_paths["markdown"]
            default_markdown = True
    return json_path, markdown_path, default_json, default_markdown


def _selected_guided_report_paths(
    *,
    repo_root: Path,
    issue: int | None,
    report_json: Path | None,
    report_markdown: Path | None,
    write_reports: bool,
) -> tuple[Path | None, Path | None, bool, bool]:
    default_paths = _default_report_paths(repo_root, issue)
    json_path = _output_path(report_json, repo_root)
    markdown_path = _output_path(report_markdown, repo_root)
    default_json = False
    default_markdown = False
    if write_reports:
        if json_path is None:
            json_path = default_paths["json"]
            default_json = True
        if markdown_path is None:
            markdown_path = default_paths["markdown"]
            default_markdown = True
    return json_path, markdown_path, default_json, default_markdown


def _selected_execution_report_paths(
    *,
    repo_root: Path,
    issue: int | None,
    report_json: Path | None,
    report_markdown: Path | None,
) -> tuple[Path, Path, bool, bool]:
    default_paths = _default_execution_report_paths(repo_root, issue)
    json_path = _output_path(report_json, repo_root)
    markdown_path = _output_path(report_markdown, repo_root)
    default_json = json_path is None
    default_markdown = markdown_path is None
    return (
        json_path or default_paths["json"],
        markdown_path or default_paths["markdown"],
        default_json,
        default_markdown,
    )


def _execution_report_path_payloads(
    *,
    json_path: Path,
    markdown_path: Path,
    repo_root: Path,
    default_json: bool,
    default_markdown: bool,
    runner: CommandRunner,
) -> dict[str, Any]:
    return {
        "json": _report_path_payload(
            json_path,
            repo_root=repo_root,
            default=default_json,
            runner=runner,
        ),
        "markdown": _report_path_payload(
            markdown_path,
            repo_root=repo_root,
            default=default_markdown,
            runner=runner,
        ),
    }


def _default_report_paths(
    repo_root: Path, issue: int | None
) -> dict[str, Path]:
    issue_part = str(issue) if issue is not None else "no-issue"
    base = repo_root / DEFAULT_REPORT_DIR / f"closure-{issue_part}"
    return {
        "json": base.with_suffix(".json"),
        "markdown": base.with_suffix(".md"),
    }


def _default_execution_report_paths(
    repo_root: Path, issue: int | None
) -> dict[str, Path]:
    issue_part = str(issue) if issue is not None else "no-issue"
    base = repo_root / DEFAULT_REPORT_DIR / f"issue-workflow-{issue_part}"
    return {
        "json": base.with_suffix(".json"),
        "markdown": base.with_suffix(".md"),
    }


def _default_execution_log_dir(repo_root: Path, issue: int | None) -> Path:
    issue_part = str(issue) if issue is not None else "no-issue"
    return repo_root / DEFAULT_REPORT_DIR / f"issue-workflow-{issue_part}-logs"


def _output_path(path: Path | None, repo_root: Path) -> Path | None:
    if path is None:
        return None
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded
    return repo_root / expanded


def _report_path_payload(
    path: Path,
    *,
    repo_root: Path,
    default: bool,
    runner: CommandRunner,
) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=False)
    try:
        relative = resolved.relative_to(repo_root)
    except ValueError:
        display = publish_safe_path(str(resolved))
        return {
            "path": str(display),
            "default": default,
            "repository_scope": "outside-repo",
            "gitignore_state": "outside-repo",
            "gitignored": None,
            "workspace_effect": "outside-repo",
            "closure_blocking": False,
            "write_allowed": True,
        }
    display = relative
    ignore = _git_ignore_status(repo_root, str(relative), runner)
    gitignored = ignore["gitignored"]
    blocking = default and gitignored is not True
    workspace_effect = "ignored"
    if gitignored is False:
        workspace_effect = "may-dirty-worktree"
    elif gitignored is None:
        workspace_effect = "unknown"
    return {
        "path": str(publish_safe_path(str(display))),
        "default": default,
        "repository_scope": "inside-repo",
        **ignore,
        "workspace_effect": workspace_effect,
        "closure_blocking": blocking,
        "write_allowed": not blocking,
    }


def _write_reports(
    report: Mapping[str, Any],
    *,
    json_path: Path | None,
    markdown_path: Path | None,
) -> None:
    report_paths = _mapping(report.get("report_paths"))
    if json_path and _report_write_allowed(report_paths, "json"):
        _write_text(json_path, json.dumps(report, indent=2) + "\n")
    if markdown_path and _report_write_allowed(report_paths, "markdown"):
        _write_text(markdown_path, render_markdown(report))


def _write_execution_reports(
    report: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> None:
    report_paths = _mapping(report.get("report_paths"))
    if _report_write_allowed(report_paths, "json"):
        _write_text(json_path, json.dumps(report, indent=2) + "\n")
    if _report_write_allowed(report_paths, "markdown"):
        _write_text(markdown_path, render_issue_workflow_markdown(report))


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())

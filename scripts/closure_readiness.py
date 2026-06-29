#!/usr/bin/env python3
"""Generate publish-safe issue closure readiness reports."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

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
) -> dict[str, Any]:
    """Return a publish-safe closure-readiness report."""
    command_runner = runner or _run_command
    root = repo_root.expanduser().resolve(strict=False)
    generated_at = now or datetime.now(timezone.utc)
    git_state_before_gates = collect_git_state(root, runner=command_runner)
    issue_state = collect_issue_state(root, issue, runner=command_runner)
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
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "operation": "closure-readiness",
        "generated_at_utc": generated_at.astimezone(timezone.utc).isoformat(),
        "repo": {
            "root": str(publish_safe_path(str(root))),
            **git_state,
        },
        "issue": issue_state,
        "processes": process_summary,
        "processes_before_gates": processes_before_gates,
        "source_artifacts": artifact_summary,
        "source_artifacts_before_gates": source_artifacts_before_gates,
        "gates": gates,
        "release_preflight": release,
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


def collect_issue_state(
    repo_root: Path,
    issue: int | None,
    *,
    runner: CommandRunner,
) -> dict[str, Any]:
    """Return linked GitHub issue metadata when requested."""
    if issue is None:
        return {
            "requested": False,
            "state": "not-requested",
            "reason": "no issue number supplied",
        }
    result = runner(
        (
            "gh",
            "issue",
            "view",
            str(issue),
            "--json",
            "number,state,title,url",
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
    return {
        "requested": True,
        "number": int(payload.get("number", issue)),
        "state": str(payload.get("state", "unknown")),
        "title": str(payload.get("title", "")),
        "url": str(payload.get("url", "")),
    }


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


def render_markdown(report: Mapping[str, Any]) -> str:
    """Render a publish-safe Markdown readiness report."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    readiness = _mapping(report.get("readiness"))
    processes = _mapping(report.get("processes"))
    artifacts = _mapping(report.get("source_artifacts"))
    gates = _mapping(report.get("gates"))
    release = _mapping(report.get("release_preflight"))
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
            f"- Worktree dirty: {_yes_no(repo.get('dirty'))}",
            f"- Lingering processes: {processes.get('state', 'unknown')} "
            f"({processes.get('total_count', 0)})",
            f"- Transient source artifacts: {artifacts.get('state', 'unknown')} "
            f"({artifacts.get('source_artifact_count', 0)})",
            f"- Release preflight: {release.get('state', 'unknown')}",
            "",
            "## GitHub Close Comment",
            "",
            "```text",
            str(report.get("close_comment", "")),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def render_human(report: Mapping[str, Any]) -> str:
    """Render a compact console summary."""
    repo = _mapping(report.get("repo"))
    readiness = _mapping(report.get("readiness"))
    gates = _mapping(report.get("gates"))
    issue = _mapping(report.get("issue"))
    lines = [
        "Closure readiness",
        f"state: {readiness.get('state', 'unknown')}",
        f"issue: {_issue_label(issue)}",
        f"branch: {repo.get('branch', 'unknown')}",
        f"upstream: {repo.get('upstream', '')} "
        f"ahead={repo.get('ahead', 0)} behind={repo.get('behind', 0)}",
        f"commit: {repo.get('head_short', '')}",
        f"worktree dirty: {_yes_no(repo.get('dirty'))}",
        f"gates: {gates.get('state', 'unknown')}",
    ]
    blockers = list(readiness.get("blocking_checks", []) or [])
    if blockers:
        lines.append("blocking: " + ", ".join(str(item) for item in blockers))
    return "\n".join(lines)


def render_close_comment(report: Mapping[str, Any]) -> str:
    """Return a concise issue-close evidence block."""
    repo = _mapping(report.get("repo"))
    issue = _mapping(report.get("issue"))
    readiness = _mapping(report.get("readiness"))
    gates = _mapping(report.get("gates"))
    processes = _mapping(report.get("processes"))
    artifacts = _mapping(report.get("source_artifacts"))
    release = _mapping(report.get("release_preflight"))
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
    ]
    if readiness.get("blocking_checks"):
        lines.append(
            "Blocking checks: "
            + ", ".join(str(item) for item in readiness["blocking_checks"])
        )
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", type=int, help="GitHub issue number to read")
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
    parser.add_argument("--json", action="store_true", help="print JSON")
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="print Markdown instead of the compact summary",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the closure-readiness helper."""
    args = parse_args(argv)
    report = build_readiness_report(
        issue=args.issue,
        run_gates=args.run_gates,
        release_preflight=args.release_preflight,
        artifact_roots=args.artifact_roots,
    )
    if args.report_json:
        _write_text(args.report_json, json.dumps(report, indent=2) + "\n")
    if args.report_markdown:
        _write_text(args.report_markdown, render_markdown(report))
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))  # noqa: T201
    elif args.markdown:
        print(render_markdown(report))  # noqa: T201
    else:
        print(render_human(report))  # noqa: T201
    return 0 if _mapping(report.get("readiness")).get("state") == "ready" else 1


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


def _tail_text(value: str, *, line_limit: int = 12) -> str:
    lines = value.splitlines()
    tail = "\n".join(lines[-line_limit:])
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


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())

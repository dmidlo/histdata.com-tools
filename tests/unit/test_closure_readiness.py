"""Tests for the local closure-readiness helper."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType
from typing import Any, Mapping, Sequence


def _module() -> ModuleType:
    script_path = (
        Path(__file__).resolve().parents[2] / "scripts" / "closure_readiness.py"
    )
    spec = importlib.util.spec_from_file_location(
        "closure_readiness",
        script_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeRunner:
    """Command runner fixture for deterministic readiness reports."""

    def __init__(
        self,
        *,
        branch: str = "dev",
        status_stdout: str = "",
        upstream_counts: str = "0\t0\n",
        check_ignore_returncode: int = 0,
        commitizen_returncode: int = 0,
        commit_returncode: int = 0,
        push_returncode: int = 0,
        post_commit_status_stdout: str = "",
        precommit_returncode: int = 0,
        precommit_returncodes: Sequence[int] | None = None,
        precommit_stdout: str = "all hooks passed\n",
        precommit_stdout_sequence: Sequence[str] | None = None,
        precommit_changes: str = "",
        precommit_file_mutations: Mapping[str, str] | None = None,
        precommit_file_mutation_sequence: (
            Sequence[Mapping[str, str]] | None
        ) = None,
        release_returncode: int = 0,
        ps_outputs: Sequence[str] = ("",),
        issue_state: str = "OPEN",
        issue_body: str = "",
        close_returncode: int = 0,
    ) -> None:
        self.branch = branch
        self.status_stdout = status_stdout
        self.upstream_counts = upstream_counts
        self.check_ignore_returncode = check_ignore_returncode
        self.commitizen_returncode = commitizen_returncode
        self.commit_returncode = commit_returncode
        self.push_returncode = push_returncode
        self.post_commit_status_stdout = post_commit_status_stdout
        self.precommit_returncode = precommit_returncode
        self.precommit_returncodes = tuple(precommit_returncodes or ())
        self.precommit_stdout = precommit_stdout
        self.precommit_stdout_sequence = tuple(precommit_stdout_sequence or ())
        self.precommit_changes = precommit_changes
        self.precommit_file_mutations = dict(precommit_file_mutations or {})
        self.precommit_file_mutation_sequence = tuple(
            dict(item) for item in precommit_file_mutation_sequence or ()
        )
        self.release_returncode = release_returncode
        self.ps_outputs = tuple(ps_outputs)
        self.issue_state = issue_state
        self.issue_body = issue_body
        self.close_returncode = close_returncode
        self.calls: list[tuple[str, ...]] = []
        self.status_calls = 0
        self.precommit_calls = 0
        self.ps_calls = 0
        self.head = "abcdef1234567890abcdef1234567890abcdef12"

    def __call__(
        self,
        command: Sequence[str],
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        args = tuple(command)
        self.calls.append(args)
        if args == ("git", "rev-parse", "--abbrev-ref", "HEAD"):
            return _completed(args, stdout=f"{self.branch}\n")
        if args == ("git", "rev-parse", "HEAD"):
            return _completed(
                args,
                stdout=f"{self.head}\n",
            )
        if args == (
            "git",
            "rev-parse",
            "--abbrev-ref",
            "--symbolic-full-name",
            "@{u}",
        ):
            return _completed(args, stdout="origin/dev\n")
        if args == (
            "git",
            "rev-list",
            "--left-right",
            "--count",
            "HEAD...@{u}",
        ):
            return _completed(args, stdout=self.upstream_counts)
        if args[:4] == ("git", "check-ignore", "-q", "--"):
            stderr = ""
            if self.check_ignore_returncode not in {0, 1}:
                stderr = "fatal: not a git repository\n"
            return _completed(
                args,
                returncode=self.check_ignore_returncode,
                stderr=stderr,
            )
        if args == ("git", "status", "--porcelain=v1", "--untracked-files=all"):
            self.status_calls += 1
            if self.precommit_changes and self.status_calls >= 11:
                return _completed(args, stdout=self.precommit_changes)
            return _completed(args, stdout=self.status_stdout)
        if args[:3] == ("git", "add", "--"):
            staged = []
            for path in args[3:]:
                staged.append(f"M  {path}")
            self.status_stdout = "\n".join(staged) + ("\n" if staged else "")
            return _completed(args)
        if args[:3] == ("git", "commit", "-m"):
            if self.commit_returncode == 0:
                self.status_stdout = self.post_commit_status_stdout
                self.upstream_counts = "1\t0\n"
                self.head = "fedcba9876543210fedcba9876543210fedcba98"
            return _completed(
                args,
                returncode=self.commit_returncode,
                stdout=(
                    "[dev fedcba9] feat(workflow): helper\n"
                    if self.commit_returncode == 0
                    else ""
                ),
                stderr="" if self.commit_returncode == 0 else "commit failed\n",
            )
        if args[:2] == ("git", "push"):
            if self.push_returncode == 0:
                self.upstream_counts = "0\t0\n"
            return _completed(
                args,
                returncode=self.push_returncode,
                stdout="pushed\n" if self.push_returncode == 0 else "",
                stderr="" if self.push_returncode == 0 else "push failed\n",
            )
        if args == ("git", "log", "-1", "--oneline", "--decorate"):
            return _completed(
                args,
                stdout=f"{self.head[:7]} (HEAD -> {self.branch}) test commit\n",
            )
        if (
            len(args) == 6
            and args[:3] == ("gh", "issue", "view")
            and args[4] == "--json"
        ):
            number = int(args[3])
            fields = set(args[5].split(","))
            payload = {
                "number": number,
                "state": self.issue_state,
                "title": "chore(v1.4.0): helper",
                "url": f"https://github.com/example/repo/issues/{number}",
            }
            if "body" in fields:
                payload["body"] = self.issue_body
            return _completed(
                args,
                stdout=json.dumps(
                    {
                        key: value
                        for key, value in payload.items()
                        if key in fields
                    }
                ),
            )
        if args[:3] == ("gh", "issue", "close"):
            if self.close_returncode == 0:
                self.issue_state = "CLOSED"
            return _completed(
                args,
                returncode=self.close_returncode,
                stdout=f"closed issue {args[3]}\n",
                stderr="" if self.close_returncode == 0 else "close failed\n",
            )
        if args == ("git", "diff", "--check"):
            return _completed(args)
        if args[:2] == (sys.executable, "scripts/sync_readme_cli_help.py"):
            return _completed(args)
        if args[:3] == (sys.executable, "-m", "histdatacom"):
            return _completed(args, stdout="usage: histdatacom\n")
        if args[:3] == (sys.executable, "-m", "pytest"):
            return _completed(args, stdout="983 passed\n")
        if args[:3] == (sys.executable, "-m", "pre_commit"):
            index = self.precommit_calls
            self.precommit_calls += 1
            returncode = (
                self.precommit_returncodes[
                    min(index, len(self.precommit_returncodes) - 1)
                ]
                if self.precommit_returncodes
                else self.precommit_returncode
            )
            stdout = (
                self.precommit_stdout_sequence[
                    min(index, len(self.precommit_stdout_sequence) - 1)
                ]
                if self.precommit_stdout_sequence
                else self.precommit_stdout
            )
            mutations = (
                self.precommit_file_mutation_sequence[
                    min(index, len(self.precommit_file_mutation_sequence) - 1)
                ]
                if self.precommit_file_mutation_sequence
                else self.precommit_file_mutations
            )
            for path, text in mutations.items():
                target = cwd / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text, encoding="utf-8")
            return _completed(
                args,
                returncode=returncode,
                stdout=(
                    "architecture-diagrams failed\n"
                    if returncode and not self.precommit_stdout_sequence
                    else stdout
                ),
            )
        if args[:4] == (sys.executable, "-m", "commitizen", "check"):
            return _completed(
                args,
                returncode=self.commitizen_returncode,
                stdout=(
                    "Commit validation: successful!\n"
                    if self.commitizen_returncode == 0
                    else ""
                ),
                stderr=(
                    ""
                    if self.commitizen_returncode == 0
                    else "commit validation failed\n"
                ),
            )
        if args == ("bash", "pypi.sh", "testpypi_preflight"):
            return _completed(
                args,
                returncode=self.release_returncode,
                stdout="local simple index passed\n",
            )
        if args == ("ps", "-axo", "pid=,comm=,args="):
            index = min(self.ps_calls, len(self.ps_outputs) - 1)
            self.ps_calls += 1
            return _completed(args, stdout=self.ps_outputs[index])
        return _completed(args)


def test_readiness_report_is_publish_safe_and_tracks_manual_seams(
    tmp_path: Path,
) -> None:
    """The report should expose closure blockers without leaking local paths."""
    module = _module()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "leftover.csv").write_text("source", encoding="utf-8")
    process_rows = (
        "101 /Users/example/venv/bin/python /Users/example/bin/pytest tests",
        "202 temporal temporal server start-dev --db-filename /Users/example/db",
        "303 python python -m histdatacom.orchestration.worker --state /Users/example/state",
        "404 python python scripts/closure_readiness.py --issue 274",
    )

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=274,
        run_gates=False,
        artifact_roots=(data_dir,),
        process_rows=process_rows,
        runner=FakeRunner(),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    rendered = json.dumps(report, sort_keys=True)

    assert report["schema_version"] == module.SCHEMA_VERSION
    assert report["issue"]["state"] == "OPEN"
    assert report["processes"]["categories"]["pytest"]["count"] == 1
    assert report["processes"]["categories"]["temporal-worker"]["count"] == 1
    assert report["source_artifacts"]["state"] == "dirty"
    assert "gates-not-run" in report["readiness"]["blocking_checks"]
    assert str(tmp_path) not in rendered
    assert "/Users/example" not in rendered


def test_gate_run_reports_precommit_generated_artifact_changes(
    tmp_path: Path,
) -> None:
    """A pre-commit generated-file drift should be machine-readable."""
    module = _module()
    runner = FakeRunner(
        precommit_returncode=1,
        precommit_changes=" M tests/architecture/packages_pyreverse.svg\n",
    )

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=274,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    precommit = [
        gate
        for gate in report["gates"]["results"]
        if gate["name"] == "pre-commit"
    ][0]

    assert report["gates"]["state"] == "fail"
    assert "gate:pre-commit" in report["readiness"]["blocking_checks"]
    assert precommit["changed_paths_after"] == [
        "tests/architecture/packages_pyreverse.svg"
    ]


def test_standalone_gate_run_formatter_mutation_reports_rerun_guidance(
    tmp_path: Path,
) -> None:
    """Standalone gate rewrites should identify formatter/tool-only drift."""
    module = _module()
    path = tmp_path / "tests" / "unit" / "test_closure_readiness.py"
    path.parent.mkdir(parents=True)
    path.write_text("before\n", encoding="utf-8")
    runner = FakeRunner(
        status_stdout=" M tests/unit/test_closure_readiness.py\n",
        precommit_returncode=1,
        precommit_stdout="black reformatted files\n",
        precommit_file_mutations={
            "tests/unit/test_closure_readiness.py": "after\n",
        },
    )

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=295,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    mutation = report["gates"]["mutation_summary"]
    item = mutation["items"][0]
    human = module.render_human(report)
    markdown = module.render_markdown(report)

    assert report["gates"]["state"] == "fail"
    assert "closure-gates-changed-files" in (
        report["readiness"]["blocking_checks"]
    )
    assert mutation["state"] == "formatter-tool"
    assert item["responsible_gates"] == ["pre-commit"]
    assert item["classification"] == "formatter-tool"
    assert item["appears_formatter_or_tool_output"] is True
    assert report["gates"]["required_rerun"]["state"] == "required"
    assert report["gates"]["rerun"]["eligible"] is True
    assert report["gates"]["rerun"]["state"] == "not-run"
    assert "python -m pytest tests/unit/test_closure_readiness.py" in (
        report["gates"]["required_rerun"]["commands"]
    )
    assert "python scripts/closure_readiness.py --run-gates" in (
        report["gates"]["required_rerun"]["commands"]
    )
    assert "--rerun-standalone-formatter-mutations" in (
        report["gates"]["rerun"]["reason"]
    )
    assert "required rerun: required" in human
    assert "Required rerun commands" in markdown


def test_standalone_gate_run_non_formatter_mutation_blocks_rerun(
    tmp_path: Path,
) -> None:
    """Standalone non-formatter rewrites should require manual inspection."""
    module = _module()
    path = tmp_path / "data" / "local-cache.sqlite3"
    path.parent.mkdir(parents=True)
    path.write_text("before\n", encoding="utf-8")
    runner = FakeRunner(
        status_stdout=" M data/local-cache.sqlite3\n",
        precommit_returncode=1,
        precommit_file_mutations={
            "data/local-cache.sqlite3": "after\n",
        },
    )

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=295,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    mutation = report["gates"]["mutation_summary"]
    item = mutation["items"][0]

    assert report["gates"]["state"] == "fail"
    assert "closure-gates-changed-files" in (
        report["readiness"]["blocking_checks"]
    )
    assert mutation["state"] == "non-formatter"
    assert mutation["formatter_tool_only"] is False
    assert item["responsible_gates"] == ["pre-commit"]
    assert item["classification"] == "non-formatter"
    assert item["appears_formatter_or_tool_output"] is False
    assert report["gates"]["required_rerun"]["state"] == "not-eligible"
    assert report["gates"]["rerun"]["eligible"] is False


def test_standalone_gate_run_formatter_rerun_success_clears_gate_blocker(
    tmp_path: Path,
) -> None:
    """Opt-in standalone reruns should clear formatter-only gate blockers."""
    module = _module()
    path = tmp_path / "tests" / "unit" / "test_closure_readiness.py"
    path.parent.mkdir(parents=True)
    path.write_text("before\n", encoding="utf-8")
    runner = FakeRunner(
        status_stdout=" M tests/unit/test_closure_readiness.py\n",
        precommit_returncodes=(1, 0),
        precommit_stdout_sequence=(
            "black reformatted files\n",
            "all hooks passed\n",
        ),
        precommit_file_mutation_sequence=(
            {"tests/unit/test_closure_readiness.py": "after\n"},
            {},
        ),
    )

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=295,
        run_gates=True,
        rerun_standalone_formatter_mutations=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert report["gates"]["state"] == "pass"
    assert report["gates"]["required_rerun"]["state"] == "passed"
    assert report["gates"]["rerun"]["state"] == "pass"
    assert "closure-gates-changed-files" not in (
        report["readiness"]["blocking_checks"]
    )
    assert "gate:pre-commit" not in report["readiness"]["blocking_checks"]
    assert "dirty-worktree" in report["readiness"]["blocking_checks"]
    assert runner.precommit_calls == 2


def test_standalone_gate_run_formatter_rerun_failure_blocks_readiness(
    tmp_path: Path,
) -> None:
    """Failed opt-in standalone reruns should keep closure blocked."""
    module = _module()
    path = tmp_path / "tests" / "unit" / "test_closure_readiness.py"
    path.parent.mkdir(parents=True)
    path.write_text("before\n", encoding="utf-8")
    runner = FakeRunner(
        status_stdout=" M tests/unit/test_closure_readiness.py\n",
        precommit_returncodes=(1, 1),
        precommit_stdout_sequence=(
            "black reformatted files\n",
            "pre-commit still failing\n",
        ),
        precommit_file_mutation_sequence=(
            {"tests/unit/test_closure_readiness.py": "after\n"},
            {},
        ),
    )

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=295,
        run_gates=True,
        rerun_standalone_formatter_mutations=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert report["gates"]["state"] == "fail"
    assert report["gates"]["required_rerun"]["state"] == "failed"
    assert report["gates"]["rerun"]["state"] == "failed"
    assert "standalone-gate-rerun-gates-failed" in (
        report["gates"]["rerun"]["blocking_checks"]
    )
    assert "standalone-gate-rerun-gates-failed" in (
        report["readiness"]["blocking_checks"]
    )


def test_gate_run_uses_final_lingering_process_check(tmp_path: Path) -> None:
    """A process spawned by a gate should block closure readiness."""
    module = _module()
    runner = FakeRunner(
        ps_outputs=(
            "",
            "303 python python -m histdatacom.orchestration.worker",
        )
    )

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=274,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert report["processes_before_gates"]["state"] == "clean"
    assert report["processes"]["state"] == "dirty"
    assert "lingering-processes" in report["readiness"]["blocking_checks"]


def test_release_preflight_is_explicit_and_can_be_included(
    tmp_path: Path,
) -> None:
    """TestPyPI local preflight should not be implicit normal closure work."""
    module = _module()
    runner = FakeRunner()

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=274,
        run_gates=True,
        release_preflight=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert report["readiness"]["state"] == "ready"
    assert report["release_preflight"]["state"] == "pass"
    assert ("bash", "pypi.sh", "testpypi_preflight") in runner.calls


def test_precheck_mode_reports_ready_without_running_gates(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Precheck should be cheap and should not claim full gate readiness."""
    module = _module()
    runner = FakeRunner()

    exit_code = module.main(
        ["--issue", "278", "--precheck"],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Closure precheck" in output
    assert "state: ready" in output
    assert not any(
        call[:3] == (sys.executable, "-m", "pytest") for call in runner.calls
    )
    assert not any(
        call[:3] == (sys.executable, "-m", "pre_commit")
        for call in runner.calls
    )


def test_issue_audit_mode_reads_issue_without_running_gates(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Issue audit should replace manual gh issue view readbacks."""
    module = _module()
    runner = FakeRunner()

    exit_code = module.main(
        ["--issue", "279", "--issue-audit"],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Closure issue audit" in output
    assert "issue: #279 OPEN" in output
    assert "title: chore(v1.4.0): helper" in output
    assert not any(
        call[:3] == (sys.executable, "-m", "pytest") for call in runner.calls
    )
    assert not any(
        call[:3] == (sys.executable, "-m", "pre_commit")
        for call in runner.calls
    )


def test_acceptance_criteria_parser_reads_checklists_and_section_bullets() -> (
    None
):
    """Issue bodies should produce stable acceptance evidence items."""
    module = _module()

    criteria = module.parse_acceptance_criteria("""
## Problem

Context text.

## Acceptance criteria

- [x] Parse issue body checklists.
- Emit Markdown evidence.
- Distinguish missing criteria.

## Context

- This bullet is outside the acceptance section.
""")
    checklist = module.parse_acceptance_criteria("""
Implementation notes.

- [ ] Global checklist item.
""")

    assert [item["id"] for item in criteria] == [
        "ac-001",
        "ac-002",
        "ac-003",
    ]
    assert criteria[0]["source"] == "checklist"
    assert criteria[0]["issue_checked"] is True
    assert criteria[1]["source"] == "bullet"
    assert criteria[2]["slug"] == "distinguish-missing-criteria"
    assert [item["text"] for item in checklist] == ["Global checklist item."]


def test_acceptance_coverage_renders_publish_safe_markdown(
    tmp_path: Path,
) -> None:
    """Explicit evidence should render without leaking local machine paths."""
    module = _module()
    issue_body = """
## Acceptance criteria

- Parse issue body checklist or acceptance-criteria bullets.
- Emit Markdown and JSON coverage evidence.
- Clearly distinguish not applicable criteria.
"""
    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=286,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=FakeRunner(issue_body=issue_body),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        acceptance_files={
            "ac-001": ("/Users/example/project/scripts/closure_readiness.py",),
        },
        acceptance_tests={
            "ac-001": ("tests/unit/test_closure_readiness.py",),
        },
        acceptance_reports={
            "*": (
                "/Users/example/project/.histdatacom/closure-readiness/"
                "closure-286.json",
            ),
        },
        acceptance_statuses={
            "ac-002": "manual",
            "ac-003": "not-applicable",
        },
        acceptance_notes={
            "ac-002": ("manual audit from /Users/example/private.txt",),
        },
    )

    markdown = module.render_markdown(report)
    coverage = report["acceptance_coverage"]

    assert coverage["state"] == "ready"
    assert coverage["counts"]["verified"] == 1
    assert coverage["counts"]["manual"] == 1
    assert coverage["counts"]["not-applicable"] == 1
    assert "## Acceptance Coverage" in markdown
    assert "| `ac-001` | verified |" in markdown
    assert ".histdatacom/closure-readiness/closure-286.json" in markdown
    assert "/Users/example" not in markdown
    assert "/Users/example" not in json.dumps(report, sort_keys=True)


def test_close_issue_refuses_missing_acceptance_criteria(
    tmp_path: Path,
) -> None:
    """Automatic close should block when required criteria lack evidence."""
    module = _module()
    runner = FakeRunner(issue_body="""
## Acceptance criteria

- Parse issue body checklists.
""")
    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=286,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    updated = module.attach_issue_close_action(
        report,
        repo_root=tmp_path,
        runner=runner,
    )

    assert report["readiness"]["state"] == "ready"
    assert report["acceptance_coverage"]["state"] == "blocked"
    assert updated["issue_close"]["state"] == "refused"
    assert "acceptance-criteria-missing" in (
        updated["issue_close"]["blocking_checks"]
    )
    assert not any(
        call[:3] == ("gh", "issue", "close") for call in runner.calls
    )


def test_close_issue_records_explicit_acceptance_override(
    tmp_path: Path,
) -> None:
    """Missing criteria may be overridden only with explicit recorded evidence."""
    module = _module()
    runner = FakeRunner(issue_body="""
## Acceptance criteria

- This criterion is intentionally deferred.
""")

    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=286,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        acceptance_missing_ok=True,
        acceptance_override_reason="deferred by maintainer",
    )
    updated = module.attach_issue_close_action(
        report,
        repo_root=tmp_path,
        runner=runner,
    )
    close_call = [
        call for call in runner.calls if call[:3] == ("gh", "issue", "close")
    ][0]

    assert report["acceptance_coverage"]["state"] == "override"
    assert updated["issue_close"]["state"] == "closed"
    assert "Acceptance override: deferred by maintainer" in close_call[-1]


def test_execute_workflow_infers_acceptance_evidence(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Executable workflow should infer files, tests, and report paths."""
    module = _module()
    runner = FakeRunner(
        status_stdout=(
            " M scripts/closure_readiness.py\n"
            " M tests/unit/test_closure_readiness.py\n"
        ),
        issue_body="""
## Acceptance criteria

- Parse issue body checklist or acceptance-criteria bullets.
- Emit Markdown and JSON coverage evidence.
""",
    )

    exit_code = module.main(
        [
            "--issue",
            "286",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): add acceptance coverage evidence",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)
    coverage = payload["acceptance_coverage"]

    assert exit_code == 0
    assert payload["readiness"]["state"] == "ready"
    assert coverage["state"] == "ready"
    assert coverage["counts"]["verified"] == 2
    assert coverage["items"][0]["files"] == [
        "scripts/closure_readiness.py",
        "tests/unit/test_closure_readiness.py",
    ]
    assert coverage["items"][0]["tests"] == [
        "tests/unit/test_closure_readiness.py"
    ]
    assert ".histdatacom/closure-readiness/issue-workflow-286.json" in (
        coverage["items"][0]["reports"]
    )
    assert payload["final_readback"]["issue"]["state"] == "CLOSED"


def test_commit_readiness_blocks_clean_tree_without_changes(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Commit readiness should not claim work exists on a clean tree."""
    module = _module()
    runner = FakeRunner()

    exit_code = module.main(
        [
            "--issue",
            "281",
            "--commit-readiness",
            "--commit-message",
            "feat(workflow): add commit readiness",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "Commit readiness" in output
    assert "state: blocked" in output
    assert "no-changes" in output
    assert any(
        call[:4] == (sys.executable, "-m", "commitizen", "check")
        for call in runner.calls
    )


def test_commit_readiness_accepts_intended_dirty_files(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Declared dirty paths plus a valid message should be ready to commit."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M README.md\n?? scripts/new_helper.py\n"
    )

    exit_code = module.main(
        [
            "--issue",
            "281",
            "--commit-readiness",
            "--commit-message",
            "feat(workflow): add commit readiness",
            "--commit-path",
            "README.md",
            "--commit-path",
            "scripts/new_helper.py",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "state: ready" in output
    assert "changes: 2 total, 0 staged, 1 unstaged, 1 untracked" in output
    assert "scope: clean" in output
    assert "git add -- README.md scripts/new_helper.py" in output
    assert "git commit -m 'feat(workflow): add commit readiness'" in output
    assert (
        "scripts/closure_readiness.py --issue 281 --workflow --close-issue"
        in (output)
    )


def test_commit_readiness_blocks_unrelated_dirty_files(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Declared scope should catch unrelated worktree changes."""
    module = _module()
    runner = FakeRunner(status_stdout=" M README.md\n M pyproject.toml\n")

    exit_code = module.main(
        [
            "--commit-readiness",
            "--commit-message",
            "feat(workflow): add commit readiness",
            "--commit-path",
            "README.md",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "state: blocked" in output
    assert "unrelated-changes" in output
    assert "unrelated: pyproject.toml" in output


def test_commit_readiness_blocks_invalid_commit_message(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Commitizen validation should be part of the readiness report."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M README.md\n",
        commitizen_returncode=1,
    )

    exit_code = module.main(
        [
            "--commit-readiness",
            "--commit-message",
            "bad message",
            "--commit-path",
            "README.md",
            "--json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["schema_version"] == module.COMMIT_READINESS_SCHEMA_VERSION
    assert payload["commit_message"]["state"] == "invalid"
    assert "commit-message-invalid" in payload["readiness"]["blocking_checks"]


def test_commit_readiness_accepts_acceptance_evidence_without_mutating(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Commit readiness should validate acceptance evidence report-only."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        issue_body="""
## Acceptance criteria

- Support acceptance evidence in commit readiness.
""",
    )

    exit_code = module.main(
        [
            "--issue",
            "291",
            "--commit-readiness",
            "--commit-message",
            "feat(workflow): support non-mutating acceptance readiness",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--acceptance-test",
            "*=tests/unit/test_closure_readiness.py",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "state: ready" in output
    assert "acceptance: ready (1/1 covered, 0 missing)" in output
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert not any(call[:3] == ("git", "commit", "-m") for call in runner.calls)
    assert not any(call[:2] == ("git", "push") for call in runner.calls)
    assert not any(
        call[:3] == ("gh", "issue", "close") for call in runner.calls
    )


def test_commit_readiness_json_includes_acceptance_coverage(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Commit readiness JSON should expose stable acceptance coverage."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        issue_body="""
## Acceptance criteria

- Include stable JSON output for automation.
""",
    )

    exit_code = module.main(
        [
            "--issue",
            "291",
            "--commit-readiness",
            "--commit-message",
            "feat(workflow): support non-mutating acceptance readiness",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--acceptance-test",
            "*=tests/unit/test_closure_readiness.py",
            "--json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["schema_version"] == module.COMMIT_READINESS_SCHEMA_VERSION
    assert payload["readiness"]["state"] == "ready"
    assert payload["acceptance_coverage"]["state"] == "ready"
    assert payload["acceptance_coverage"]["criteria_count"] == 1
    assert payload["acceptance_coverage"]["covered_count"] == 1
    assert payload["acceptance_coverage"]["missing_count"] == 0
    assert payload["acceptance_coverage"]["items"][0]["tests"] == [
        "tests/unit/test_closure_readiness.py"
    ]
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert not any(call[:2] == ("git", "push") for call in runner.calls)
    assert not any(
        call[:3] == ("gh", "issue", "close") for call in runner.calls
    )


def test_commit_readiness_blocks_missing_acceptance_evidence(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Missing acceptance evidence should block non-mutating readiness."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        issue_body="""
## Acceptance criteria

- Cover the first criterion.
- Cover the second criterion.
""",
    )

    exit_code = module.main(
        [
            "--issue",
            "291",
            "--commit-readiness",
            "--commit-message",
            "feat(workflow): support non-mutating acceptance readiness",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--acceptance-test",
            "ac-001=tests/unit/test_closure_readiness.py",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "state: blocked" in output
    assert "acceptance: blocked (1/2 covered, 1 missing)" in output
    assert "acceptance-criteria-missing" in output
    assert "acceptance:ac-002" in output
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert not any(
        call[:3] == ("gh", "issue", "close") for call in runner.calls
    )


def test_commit_readiness_blocks_invalid_message_and_scope_with_acceptance(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Acceptance evidence should not hide commit message or scope blockers."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M README.md\n M pyproject.toml\n",
        commitizen_returncode=1,
        issue_body="""
## Acceptance criteria

- Keep existing commit readiness blockers.
""",
    )

    exit_code = module.main(
        [
            "--issue",
            "291",
            "--commit-readiness",
            "--commit-message",
            "bad message",
            "--commit-path",
            "README.md",
            "--acceptance-test",
            "*=tests/unit/test_closure_readiness.py",
            "--json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)
    blockers = payload["readiness"]["blocking_checks"]

    assert exit_code == 1
    assert payload["acceptance_coverage"]["state"] == "ready"
    assert payload["commit_message"]["state"] == "invalid"
    assert payload["scope"]["state"] == "dirty-unrelated"
    assert "commit-message-invalid" in blockers
    assert "unrelated-changes" in blockers
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert not any(
        call[:3] == ("gh", "issue", "close") for call in runner.calls
    )


def test_commit_readiness_blocks_upstream_behind(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Operators should not prepare commits while dev is behind upstream."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M README.md\n",
        upstream_counts="0\t1\n",
    )

    exit_code = module.main(
        [
            "--commit-readiness",
            "--commit-message",
            "feat(workflow): add commit readiness",
            "--commit-path",
            "README.md",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "upstream: origin/dev ahead=0 behind=1" in output
    assert "upstream-behind" in output


def test_push_readiness_reports_ready_to_push_state(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Push readiness should be ready when dev is clean and ahead."""
    module = _module()
    runner = FakeRunner(upstream_counts="1\t0\n")

    exit_code = module.main(
        ["--issue", "281", "--push-readiness"],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Push readiness" in output
    assert "state: ready" in output
    assert "upstream: origin/dev ahead=1 behind=0" in output
    assert "git push origin dev" in output
    assert (
        "scripts/closure_readiness.py --issue 281 --workflow --close-issue"
        in (output)
    )
    assert not any(
        call[:4] == (sys.executable, "-m", "commitizen", "check")
        for call in runner.calls
    )


def test_execute_workflow_runs_ready_sequence_and_closes_issue(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Executable workflow should commit, push, run gates, and close."""
    module = _module()
    runner = FakeRunner(
        status_stdout=(
            " M scripts/closure_readiness.py\n"
            " M tests/unit/test_closure_readiness.py\n"
        )
    )

    exit_code = module.main(
        [
            "--issue",
            "284",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): add executable workflow",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["schema_version"] == module.ISSUE_WORKFLOW_SCHEMA_VERSION
    assert payload["readiness"]["state"] == "ready"
    assert payload["final_readback"]["issue"]["state"] == "CLOSED"
    assert payload["release_preflight"]["policy"] == (
        "not-run-for-non-release-work"
    )
    assert (
        tmp_path
        / ".histdatacom"
        / "closure-readiness"
        / "issue-workflow-284.json"
    ).exists()
    assert (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-284.json"
    ).exists()
    assert any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert any(call[:3] == ("git", "commit", "-m") for call in runner.calls)
    assert any(call[:2] == ("git", "push") for call in runner.calls)
    assert any(
        call[:3] == (sys.executable, "-m", "pytest") for call in runner.calls
    )
    assert any(call[:3] == ("gh", "issue", "close") for call in runner.calls)


def test_execute_workflow_default_prints_compact_closeout(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Default execution output should be the final closeout, not raw evidence."""
    module = _module()
    runner = FakeRunner(
        status_stdout=(
            " M scripts/closure_readiness.py\n"
            " M tests/unit/test_closure_readiness.py\n"
        ),
        issue_body="""
## Acceptance criteria

- Print one compact closeout after execution.
""",
    )

    exit_code = module.main(
        [
            "--issue",
            "288",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "feat(workflow): print compact closeout",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out
    json_path = (
        tmp_path
        / ".histdatacom"
        / "closure-readiness"
        / "issue-workflow-288.json"
    )
    full_report = json.loads(json_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "Issue workflow report summary" in output
    assert "Issue workflow execution" not in output
    assert "accepted: yes" in output
    assert "issue: #288 CLOSED" in output
    assert "branch: dev -> origin/dev (aligned) ahead=0 behind=0" in output
    assert "commit: fedcba9 (HEAD -> dev) test commit" in output
    assert "worktree dirty: no" in output
    assert "pre-mutation gates: pass" in output
    assert "closure: accepted" in output
    assert "issue close: closed" in output
    assert "acceptance: ready (1/1 covered, 0 missing)" in output
    assert "report paths: ready" in output
    assert "runtime/process health: clean (0)" in output
    assert "reports:" in output
    assert (
        "json: .histdatacom/closure-readiness/issue-workflow-288.json "
        "[ignored; write]"
    ) in output
    assert (
        "markdown: .histdatacom/closure-readiness/issue-workflow-288.md "
        "[ignored; write]"
    ) in output
    assert '"commands":' not in output
    assert full_report["schema_version"] == module.ISSUE_WORKFLOW_SCHEMA_VERSION


def test_execute_workflow_json_prints_compact_closeout_payload(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """JSON execution output should be compact while full evidence is saved."""
    module = _module()
    runner = FakeRunner(
        status_stdout=(
            " M scripts/closure_readiness.py\n"
            " M tests/unit/test_closure_readiness.py\n"
        ),
        issue_body="""
## Acceptance criteria

- Keep compact JSON scriptable.
""",
    )

    exit_code = module.main(
        [
            "--issue",
            "288",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "feat(workflow): print compact closeout",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
            "--json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    summary = json.loads(capsys.readouterr().out)
    json_path = (
        tmp_path
        / ".histdatacom"
        / "closure-readiness"
        / "issue-workflow-288.json"
    )
    full_report = json.loads(json_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert summary["schema_version"] == (
        module.ISSUE_WORKFLOW_SUMMARY_SCHEMA_VERSION
    )
    assert summary["source_schema_version"] == (
        module.ISSUE_WORKFLOW_SCHEMA_VERSION
    )
    assert summary["accepted"] is True
    assert summary["issue"]["label"] == "#288 CLOSED"
    assert summary["repo"]["upstream_state"] == "aligned"
    assert summary["commit"]["summary"].startswith("fedcba9 ")
    assert summary["pre_mutation_gates"]["state"] == "pass"
    assert summary["closure"]["issue_close_state"] == "closed"
    assert summary["acceptance_coverage"]["state"] == "ready"
    assert summary["report_paths"]["state"] == "ready"
    assert summary["report_paths"]["outputs"]["json"]["gitignore_state"] == (
        "ignored"
    )
    assert summary["process_health"]["after"]["state"] == "clean"
    assert "commands" not in summary
    assert "closure_report" not in summary
    assert full_report["schema_version"] == module.ISSUE_WORKFLOW_SCHEMA_VERSION
    assert full_report["commands"]


def test_execute_workflow_streams_progress_and_records_phase_evidence(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Executable workflow progress should be live and saved in evidence."""
    module = _module()
    runner = FakeRunner(
        status_stdout=(
            " M scripts/closure_readiness.py\n"
            " M tests/unit/test_closure_readiness.py\n"
        ),
        issue_body="""
## Acceptance criteria

- Stream phase progress.
- Record progress evidence.
""",
    )

    exit_code = module.main(
        [
            "--issue",
            "289",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "feat(workflow): stream executable progress",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    captured = capsys.readouterr()
    report_path = (
        tmp_path
        / ".histdatacom"
        / "closure-readiness"
        / "issue-workflow-289.json"
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    progress = report["workflow_progress"]
    phases = {phase["phase"]: phase for phase in progress["phases"]}

    assert exit_code == 0
    assert "Issue workflow report summary" in captured.out
    assert "progress: completed" in captured.out
    assert "issue-workflow progress:" in captured.err
    assert "phase=initial-readiness status=started" in captured.err
    assert "phase=pre-mutation-gates status=started" in captured.err
    assert "phase=staging status=started" in captured.err
    assert "phase=commit status=completed" in captured.err
    assert "phase=push-readiness status=completed" in captured.err
    assert "phase=push status=completed" in captured.err
    assert "phase=closure-gates status=completed" in captured.err
    assert "phase=issue-close status=completed" in captured.err
    assert "phase=final-readback status=completed" in captured.err
    assert "phase=report-writing status=completed" in captured.err
    assert "command=git commit" in captured.err
    assert "log=.histdatacom/closure-readiness/issue-workflow-289-logs/" in (
        captured.err
    )
    assert progress["state"] == "completed"
    assert progress["stream"] == "stderr-line"
    assert progress["event_count"] >= 20
    assert progress["elapsed_seconds"] >= 0
    assert {
        "report-paths",
        "initial-readiness",
        "pre-mutation-gates",
        "staging",
        "commit",
        "push-readiness",
        "push",
        "closure-gates",
        "issue-close",
        "final-readback",
        "report-writing",
    }.issubset(phases)
    assert phases["commit"]["status"] == "completed"
    assert phases["commit"]["duration_seconds"] >= 0
    assert phases["commit"]["command_label"] == "git commit"


def test_execute_workflow_blocked_progress_is_streamed_and_recorded(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Blocked workflow progress should identify the blocking phase."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        precommit_returncode=1,
    )

    exit_code = module.main(
        [
            "--issue",
            "289",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "feat(workflow): stream executable progress",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    progress = payload["workflow_progress"]
    phases = {phase["phase"]: phase for phase in progress["phases"]}

    assert exit_code == 1
    assert payload["readiness"]["state"] == "blocked"
    assert "phase=pre-mutation-gates status=blocked" in captured.err
    assert "phase=closure-gates status=skipped" in captured.err
    assert "phase=issue-close status=skipped" in captured.err
    assert "phase=staging status=started" not in captured.err
    assert progress["state"] == "blocked"
    assert phases["pre-mutation-gates"]["status"] == "blocked"
    assert phases["closure-gates"]["status"] == "skipped"
    assert phases["issue-close"]["status"] == "skipped"


def test_execute_workflow_json_stdout_stays_parseable_with_progress(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Live progress should not pollute compact JSON stdout."""
    module = _module()
    runner = FakeRunner(status_stdout=" M scripts/closure_readiness.py\n")

    exit_code = module.main(
        [
            "--issue",
            "289",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): stream executable progress",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    captured = capsys.readouterr()
    summary = json.loads(captured.out)

    assert exit_code == 0
    assert summary["schema_version"] == (
        module.ISSUE_WORKFLOW_SUMMARY_SCHEMA_VERSION
    )
    assert summary["workflow_progress"]["state"] == "completed"
    assert summary["workflow_progress"]["stream"] == "stderr-line"
    assert summary["workflow_progress"]["slow_phases"]
    assert "commands" not in summary
    assert captured.err.startswith("issue-workflow progress:")


def test_execute_workflow_quiet_progress_suppresses_stderr_but_records(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Quiet progress should keep automation quiet without dropping evidence."""
    module = _module()
    runner = FakeRunner(status_stdout=" M scripts/closure_readiness.py\n")

    exit_code = module.main(
        [
            "--issue",
            "289",
            "--execute-workflow",
            "--quiet-progress",
            "--commit-message",
            "feat(workflow): stream executable progress",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    report_path = (
        tmp_path
        / ".histdatacom"
        / "closure-readiness"
        / "issue-workflow-289.json"
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured.err == ""
    assert summary["workflow_progress"]["state"] == "completed"
    assert summary["workflow_progress"]["stream"] == "quiet"
    assert report["workflow_progress"]["state"] == "completed"
    assert report["workflow_progress"]["stream"] == "quiet"


def test_execute_workflow_pre_mutation_gates_run_before_git_mutation(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Opt-in pre-mutation gates should pass before staging begins."""
    module = _module()
    runner = FakeRunner(status_stdout=" M scripts/closure_readiness.py\n")

    exit_code = module.main(
        [
            "--issue",
            "285",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "feat(workflow): add pre-mutation gates",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)
    first_pytest = next(
        index
        for index, call in enumerate(runner.calls)
        if call[:3] == (sys.executable, "-m", "pytest")
    )
    first_precommit = next(
        index
        for index, call in enumerate(runner.calls)
        if call[:3] == (sys.executable, "-m", "pre_commit")
    )
    first_add = next(
        index
        for index, call in enumerate(runner.calls)
        if call[:3] == ("git", "add", "--")
    )
    markdown = module.render_issue_workflow_markdown(payload)

    assert exit_code == 0
    assert payload["readiness"]["state"] == "ready"
    assert payload["pre_mutation_gates"]["enabled"] is True
    assert payload["pre_mutation_gates"]["state"] == "pass"
    assert payload["pre_mutation_gates"]["gates"]["state"] == "pass"
    assert first_pytest < first_add
    assert first_precommit < first_add
    assert "## Pre-Mutation Gates" in markdown
    assert "- State: pass" in markdown
    assert any(call[:2] == ("git", "push") for call in runner.calls)
    assert payload["final_readback"]["issue"]["state"] == "CLOSED"


def test_execute_workflow_pre_mutation_gate_failure_blocks_mutation(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Failing pre-mutation gates should stop before git add."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        precommit_returncode=1,
    )

    exit_code = module.main(
        [
            "--issue",
            "285",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "feat(workflow): add pre-mutation gates",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["readiness"]["state"] == "blocked"
    assert payload["pre_mutation_gates"]["state"] == "blocked"
    assert (
        "pre-mutation-gates-failed" in payload["readiness"]["blocking_checks"]
    )
    assert "pre-mutation-gate:pre-commit" in (
        payload["pre_mutation_gates"]["blocking_checks"]
    )
    assert any(
        call[:3] == (sys.executable, "-m", "pre_commit")
        for call in runner.calls
    )
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert not any(call[:3] == ("git", "commit", "-m") for call in runner.calls)
    assert not any(call[:2] == ("git", "push") for call in runner.calls)


def test_execute_workflow_pre_mutation_file_change_blocks_mutation(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Pre-mutation gates should catch rewrites of already-dirty files."""
    module = _module()
    path = tmp_path / "scripts" / "closure_readiness.py"
    path.parent.mkdir(parents=True)
    path.write_text("before\n", encoding="utf-8")
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        precommit_file_mutations={
            "scripts/closure_readiness.py": "after\n",
        },
    )

    exit_code = module.main(
        [
            "--issue",
            "285",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "feat(workflow): add pre-mutation gates",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["readiness"]["state"] == "blocked"
    assert "pre-mutation-gates-changed-files" in (
        payload["readiness"]["blocking_checks"]
    )
    assert payload["pre_mutation_gates"]["changed_paths_after"] == [
        "scripts/closure_readiness.py"
    ]
    assert payload["pre_mutation_gates"]["fingerprint_changed_paths"] == [
        "scripts/closure_readiness.py"
    ]
    assert path.read_text(encoding="utf-8") == "after\n"
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert not any(call[:3] == ("git", "commit", "-m") for call in runner.calls)


def test_execute_workflow_pre_mutation_formatter_change_reports_rerun_guidance(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Formatter-only gate rewrites should identify the gate and rerun path."""
    module = _module()
    path = tmp_path / "tests" / "unit" / "test_closure_readiness.py"
    path.parent.mkdir(parents=True)
    path.write_text("before\n", encoding="utf-8")
    runner = FakeRunner(
        status_stdout=" M tests/unit/test_closure_readiness.py\n",
        precommit_returncode=1,
        precommit_stdout="black reformatted files\n",
        precommit_file_mutations={
            "tests/unit/test_closure_readiness.py": "after\n",
        },
    )

    exit_code = module.main(
        [
            "--issue",
            "292",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "fix(workflow): classify formatter mutations",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)
    mutation = payload["pre_mutation_gates"]["mutation_summary"]
    item = mutation["items"][0]

    assert exit_code == 1
    assert payload["pre_mutation_gates"]["state"] == "blocked"
    assert mutation["state"] == "formatter-tool"
    assert mutation["formatter_tool_only"] is True
    assert item["responsible_gates"] == ["pre-commit"]
    assert item["classification"] == "formatter-tool"
    assert item["appears_formatter_or_tool_output"] is True
    assert payload["pre_mutation_gates"]["required_rerun"]["state"] == (
        "required"
    )
    assert payload["pre_mutation_gates"]["rerun"]["eligible"] is True
    assert payload["pre_mutation_gates"]["rerun"]["state"] == "not-run"
    assert "python -m pytest tests/unit/test_closure_readiness.py" in (
        payload["pre_mutation_gates"]["required_rerun"]["commands"]
    )
    assert "python scripts/closure_readiness.py --run-gates" in (
        payload["pre_mutation_gates"]["required_rerun"]["commands"]
    )
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)


def test_execute_workflow_pre_mutation_non_formatter_change_blocks_rerun(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Non-formatter gate rewrites should require manual inspection."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M data/local-cache.sqlite3\n",
        precommit_returncode=1,
        precommit_file_mutations={
            "data/local-cache.sqlite3": "binary-ish\n",
        },
    )

    exit_code = module.main(
        [
            "--issue",
            "292",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "fix(workflow): classify non formatter mutations",
            "--commit-path",
            "data/local-cache.sqlite3",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)
    mutation = payload["pre_mutation_gates"]["mutation_summary"]
    item = mutation["items"][0]

    assert exit_code == 1
    assert payload["pre_mutation_gates"]["state"] == "blocked"
    assert mutation["state"] == "non-formatter"
    assert mutation["formatter_tool_only"] is False
    assert item["responsible_gates"] == ["pre-commit"]
    assert item["classification"] == "non-formatter"
    assert item["appears_formatter_or_tool_output"] is False
    assert payload["pre_mutation_gates"]["required_rerun"]["state"] == (
        "not-eligible"
    )
    assert payload["pre_mutation_gates"]["rerun"]["eligible"] is False
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)


def test_execute_workflow_pre_mutation_formatter_rerun_success_allows_commit(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Opt-in formatter reruns should permit staging only after verification."""
    module = _module()
    path = tmp_path / "tests" / "unit" / "test_closure_readiness.py"
    path.parent.mkdir(parents=True)
    path.write_text("before\n", encoding="utf-8")
    runner = FakeRunner(
        status_stdout=" M tests/unit/test_closure_readiness.py\n",
        precommit_returncodes=(1, 0, 0),
        precommit_stdout_sequence=(
            "black reformatted files\n",
            "all hooks passed\n",
            "all hooks passed\n",
        ),
        precommit_file_mutation_sequence=(
            {"tests/unit/test_closure_readiness.py": "after\n"},
            {},
        ),
    )

    exit_code = module.main(
        [
            "--issue",
            "292",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--rerun-formatter-mutations",
            "--commit-message",
            "fix(workflow): rerun formatter mutations",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["readiness"]["state"] == "ready"
    assert payload["pre_mutation_gates"]["state"] == "pass"
    assert payload["pre_mutation_gates"]["required_rerun"]["state"] == (
        "passed"
    )
    assert payload["pre_mutation_gates"]["rerun"]["state"] == "pass"
    assert (
        payload["pre_mutation_gates"]["rerun"]["focused_tests"]["state"]
        == "pass"
    )
    assert payload["pre_mutation_gates"]["rerun"]["changed_paths_after"] == []
    assert runner.precommit_calls >= 2
    assert any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert any(call[:3] == ("git", "commit", "-m") for call in runner.calls)
    assert any(call[:2] == ("git", "push") for call in runner.calls)


def test_execute_workflow_pre_mutation_formatter_rerun_failure_blocks_commit(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Failed opt-in formatter reruns should still block git mutation."""
    module = _module()
    path = tmp_path / "tests" / "unit" / "test_closure_readiness.py"
    path.parent.mkdir(parents=True)
    path.write_text("before\n", encoding="utf-8")
    runner = FakeRunner(
        status_stdout=" M tests/unit/test_closure_readiness.py\n",
        precommit_returncodes=(1, 1),
        precommit_stdout_sequence=(
            "black reformatted files\n",
            "pre-commit still failing\n",
        ),
        precommit_file_mutation_sequence=(
            {"tests/unit/test_closure_readiness.py": "after\n"},
            {},
        ),
    )

    exit_code = module.main(
        [
            "--issue",
            "292",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--rerun-formatter-mutations",
            "--commit-message",
            "fix(workflow): rerun formatter mutations",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["readiness"]["state"] == "blocked"
    assert payload["pre_mutation_gates"]["state"] == "blocked"
    assert payload["pre_mutation_gates"]["required_rerun"]["state"] == "failed"
    assert payload["pre_mutation_gates"]["rerun"]["state"] == "failed"
    assert "pre-mutation-rerun-gates-failed" in (
        payload["pre_mutation_gates"]["rerun"]["blocking_checks"]
    )
    assert "pre-mutation-rerun-gates-failed" in (
        payload["readiness"]["blocking_checks"]
    )
    assert runner.precommit_calls == 2
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)


def test_execute_workflow_blocks_clean_tree_without_mutation(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Executable workflow should stop when commit readiness is blocked."""
    module = _module()
    runner = FakeRunner()

    exit_code = module.main(
        [
            "--issue",
            "284",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): add executable workflow",
            "--commit-path",
            "scripts/closure_readiness.py",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "Issue workflow report summary" in output
    assert "state: blocked" in output
    assert "no-changes" in output
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert not any(call[:3] == ("git", "commit", "-m") for call in runner.calls)


def test_execute_workflow_blocks_invalid_commit_message(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Commitizen failure should block before staging."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        commitizen_returncode=1,
    )

    exit_code = module.main(
        [
            "--issue",
            "284",
            "--execute-workflow",
            "--commit-message",
            "bad message",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["readiness"]["state"] == "blocked"
    assert "commit-message-invalid" in payload["readiness"]["blocking_checks"]
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)


def test_execute_workflow_blocks_unrelated_dirty_paths(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Declared commit paths should prevent accidental unrelated commits."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n M pyproject.toml\n"
    )

    exit_code = module.main(
        [
            "--issue",
            "284",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): add executable workflow",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["readiness"]["state"] == "blocked"
    assert "unrelated-changes" in payload["readiness"]["blocking_checks"]
    assert payload["commit_readiness"]["scope"]["unrelated_paths"] == [
        "pyproject.toml"
    ]
    assert not any(call[:3] == ("git", "add", "--") for call in runner.calls)


def test_execute_workflow_stops_on_commit_command_failure(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Command failures should stop the mutating sequence."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        commit_returncode=1,
    )

    exit_code = module.main(
        [
            "--issue",
            "284",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): add executable workflow",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["readiness"]["state"] == "failed"
    assert "command:git-commit" in payload["readiness"]["blocking_checks"]
    assert any(call[:3] == ("git", "add", "--") for call in runner.calls)
    assert any(call[:3] == ("git", "commit", "-m") for call in runner.calls)
    assert not any(call[:2] == ("git", "push") for call in runner.calls)


def test_execute_workflow_stops_when_push_readiness_fails(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Generated dirty files after commit should block push."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        post_commit_status_stdout=" M generated.txt\n",
    )

    exit_code = module.main(
        [
            "--issue",
            "284",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): add executable workflow",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["readiness"]["state"] == "blocked"
    assert "dirty-worktree" in payload["readiness"]["blocking_checks"]
    assert payload["push_readiness"]["readiness"]["state"] == "blocked"
    assert not any(call[:2] == ("git", "push") for call in runner.calls)


def test_execute_workflow_reports_issue_close_failure(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Issue close failure should be visible after commit and push."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        close_returncode=1,
    )

    exit_code = module.main(
        [
            "--issue",
            "284",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): add executable workflow",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["readiness"]["state"] == "failed"
    assert "closure-workflow" in payload["readiness"]["blocking_checks"]
    assert payload["closure_summary"]["issue_close"]["state"] == "failed"
    assert payload["final_readback"]["issue"]["state"] == "OPEN"
    assert any(call[:2] == ("git", "push") for call in runner.calls)


def test_execute_workflow_writes_full_logs_but_bounds_report_tails(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Reports should expose bounded tails while ignored logs keep full output."""
    module = _module()
    long_stdout = "\n".join(f"line-{index:03d}" for index in range(30)) + "\n"
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        precommit_stdout=long_stdout,
    )

    exit_code = module.main(
        [
            "--issue",
            "284",
            "--execute-workflow",
            "--commit-message",
            "feat(workflow): add executable workflow",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)
    precommit = [
        command
        for command in payload["commands"]
        if command["name"] == "gate-pre-commit"
    ][0]
    log_path = tmp_path / precommit["log_path"]
    log_text = log_path.read_text(encoding="utf-8")

    assert exit_code == 0
    assert "line-029" in precommit["stdout_tail"]
    assert "line-000" not in precommit["stdout_tail"]
    assert "line-000" in log_text
    assert "line-029" in log_text


def test_print_close_comment_outputs_only_comment(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Operators should not need an ad hoc JSON extraction snippet."""
    module = _module()

    exit_code = module.main(
        ["--issue", "278", "--run-gates", "--print-close-comment"],
        repo_root=tmp_path,
        runner=FakeRunner(),
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert output.startswith("Closure readiness: ready\n")
    assert "Issue: #278 OPEN" in output
    assert "Closure readiness\n" not in output


def test_write_reports_uses_issue_default_paths(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Operators should not have to hand-assemble issue report paths."""
    module = _module()

    exit_code = module.main(
        ["--issue", "279", "--run-gates", "--write-reports"],
        repo_root=tmp_path,
        runner=FakeRunner(),
    )
    output = capsys.readouterr().out
    json_path = (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.json"
    )
    markdown_path = (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.md"
    )
    payload = json.loads(json_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert json_path.exists()
    assert markdown_path.exists()
    assert "reports:" in output
    assert payload["report_paths"]["json"]["path"] == (
        ".histdatacom/closure-readiness/closure-279.json"
    )
    assert payload["report_paths"]["json"]["default"] is True
    assert payload["report_paths"]["json"]["gitignore_state"] == "ignored"
    assert payload["report_paths"]["json"]["write_allowed"] is True
    assert payload["report_paths"]["markdown"]["default"] is True
    assert payload["report_paths"]["markdown"]["gitignore_state"] == "ignored"
    assert payload["readiness"]["state"] == "ready"


def test_write_reports_blocks_unignored_default_paths(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Default reports should not dirty the repo when ignore rules drift."""
    module = _module()
    runner = FakeRunner(check_ignore_returncode=1)

    exit_code = module.main(
        ["--issue", "279", "--run-gates", "--write-reports", "--json"],
        repo_root=tmp_path,
        runner=runner,
    )
    payload = json.loads(capsys.readouterr().out)
    json_path = (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.json"
    )
    markdown_path = (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.md"
    )

    assert exit_code == 1
    assert payload["readiness"]["state"] == "blocked"
    assert (
        "report-path-not-ignored:json"
        in payload["readiness"]["blocking_checks"]
    )
    assert (
        "report-path-not-ignored:markdown"
        in payload["precheck"]["blocking_checks"]
    )
    assert payload["report_paths"]["json"]["gitignore_state"] == "not-ignored"
    assert payload["report_paths"]["json"]["write_allowed"] is False
    assert not json_path.exists()
    assert not markdown_path.exists()


def test_explicit_unignored_report_path_is_marked_but_allowed(
    tmp_path: Path,
) -> None:
    """Explicit report paths keep working but disclose dirty-worktree risk."""
    module = _module()
    custom_json = tmp_path / "custom" / "closure.json"

    exit_code = module.main(
        [
            "--issue",
            "279",
            "--run-gates",
            "--report-json",
            str(custom_json),
        ],
        repo_root=tmp_path,
        runner=FakeRunner(check_ignore_returncode=1),
    )
    payload = json.loads(custom_json.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["readiness"]["state"] == "ready"
    assert payload["report_paths"]["json"]["default"] is False
    assert payload["report_paths"]["json"]["gitignore_state"] == "not-ignored"
    assert payload["report_paths"]["json"]["workspace_effect"] == (
        "may-dirty-worktree"
    )
    assert payload["report_paths"]["json"]["write_allowed"] is True
    assert (
        "report-path-may-dirty-worktree:json"
        in payload["readiness"]["warnings"]
    )


def test_explicit_report_path_overrides_default_json_path(
    tmp_path: Path,
) -> None:
    """Default report writing should preserve explicit path overrides."""
    module = _module()
    custom_json = tmp_path / "custom" / "closure.json"

    exit_code = module.main(
        [
            "--issue",
            "279",
            "--run-gates",
            "--write-reports",
            "--report-json",
            str(custom_json),
        ],
        repo_root=tmp_path,
        runner=FakeRunner(),
    )
    default_json = (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.json"
    )
    default_markdown = (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.md"
    )
    payload = json.loads(custom_json.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert custom_json.exists()
    assert not default_json.exists()
    assert default_markdown.exists()
    assert payload["report_paths"]["json"]["default"] is False
    assert payload["report_paths"]["markdown"]["default"] is True
    assert payload["report_paths"]["json"]["gitignore_state"] == "ignored"
    assert payload["report_paths"]["markdown"]["write_allowed"] is True


def test_close_issue_refuses_when_readiness_is_blocked(tmp_path: Path) -> None:
    """The close action should not run when gates have not passed."""
    module = _module()
    runner = FakeRunner()
    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=278,
        run_gates=False,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    updated = module.attach_issue_close_action(
        report,
        repo_root=tmp_path,
        runner=runner,
    )

    assert updated["issue_close"]["state"] == "refused"
    assert "gates-not-run" in updated["issue_close"]["blocking_checks"]
    assert not any(
        call[:3] == ("gh", "issue", "close") for call in runner.calls
    )


def test_close_issue_posts_comment_and_reads_back_final_state(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Ready closure can close the issue and include post-close readback."""
    module = _module()
    runner = FakeRunner()

    exit_code = module.main(
        ["--issue", "278", "--run-gates", "--close-issue"],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out
    close_calls = [
        call for call in runner.calls if call[:3] == ("gh", "issue", "close")
    ]

    assert exit_code == 0
    assert len(close_calls) == 1
    assert close_calls[0][3] == "278"
    assert "Closure readiness: ready" in close_calls[0][-1]
    assert "issue close: closed" in output
    assert "issue final: #278 CLOSED" in output


def test_guided_workflow_runs_gates_writes_reports_and_closes(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Guided workflow should replace manual precheck/gates/report/close order."""
    module = _module()
    runner = FakeRunner()

    exit_code = module.main(
        ["--issue", "279", "--workflow", "--close-issue"],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out
    close_calls = [
        call for call in runner.calls if call[:3] == ("gh", "issue", "close")
    ]

    assert exit_code == 0
    assert "workflow: ready" in output
    assert "issue close: closed" in output
    assert "[ignored; write]" in output
    assert (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.json"
    ).exists()
    assert (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.md"
    ).exists()
    assert len(close_calls) == 1
    assert any(
        call[:3] == (sys.executable, "-m", "pytest") for call in runner.calls
    )
    assert any(
        call[:3] == (sys.executable, "-m", "pre_commit")
        for call in runner.calls
    )


def test_guided_workflow_stops_before_gates_when_precheck_is_blocked(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Guided workflow should avoid expensive gates when local state is blocked."""
    module = _module()
    runner = FakeRunner(status_stdout=" M README.md\n")

    exit_code = module.main(
        ["--issue", "279", "--workflow"],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "workflow: blocked" in output
    assert "dirty-worktree" in output
    assert not any(
        call[:3] == (sys.executable, "-m", "pytest") for call in runner.calls
    )
    assert not any(
        call[:3] == (sys.executable, "-m", "pre_commit")
        for call in runner.calls
    )


def test_guided_workflow_refuses_close_from_non_dev_branch(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Guided close should enforce the dev workflow branch."""
    module = _module()
    runner = FakeRunner(branch="feature")

    exit_code = module.main(
        ["--issue", "279", "--workflow", "--close-issue"],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "workflow: blocked" in output
    assert "not-dev-branch" in output
    assert not any(
        call[:3] == ("gh", "issue", "close") for call in runner.calls
    )


def test_guided_workflow_blocks_unignored_default_report_paths(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Guided workflow should catch report ignore drift before gates/close."""
    module = _module()
    runner = FakeRunner(check_ignore_returncode=1)

    exit_code = module.main(
        ["--issue", "279", "--workflow", "--close-issue"],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "workflow: blocked" in output
    assert "report-path-not-ignored:json" in output
    assert "json: .histdatacom/closure-readiness/closure-279.json" in output
    assert "[not-ignored; skip]" in output
    assert not (
        tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.json"
    ).exists()
    assert not any(
        call[:3] == (sys.executable, "-m", "pytest") for call in runner.calls
    )
    assert not any(
        call[:3] == ("gh", "issue", "close") for call in runner.calls
    )


def test_summarize_report_outputs_key_fields_without_live_commands(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Saved reports should not require ad hoc Python snippets to inspect."""
    module = _module()
    runner = FakeRunner()
    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=279,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    report = module.attach_issue_close_action(
        report,
        repo_root=tmp_path,
        runner=runner,
    )
    report_path = tmp_path / "closure.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    runner.calls.clear()

    exit_code = module.main(
        ["--summarize-report", str(report_path)],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Closure report summary" in output
    assert "state: ready" in output
    assert "gates: pass" in output
    assert "issue close: closed" in output
    assert "issue final: #279 CLOSED" in output
    assert runner.calls == []


def test_summarize_report_json_returns_summary_payload(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Report summary JSON should expose stable key status fields."""
    module = _module()
    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=279,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=FakeRunner(),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    report_path = tmp_path / "closure.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    exit_code = module.main(
        ["--summarize-report", str(report_path), "--json"],
        repo_root=tmp_path,
        runner=FakeRunner(),
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["schema_version"] == module.SUMMARY_SCHEMA_VERSION
    assert payload["readiness"]["state"] == "ready"
    assert payload["gates"]["state"] == "pass"
    assert payload["issue"]["label"] == "#279 OPEN"


def test_summarize_issue_workflow_report_outputs_final_readback(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Execution reports should summarize final readback, not closure defaults."""
    module = _module()
    runner = FakeRunner(
        status_stdout=(
            " M scripts/closure_readiness.py\n"
            " M tests/unit/test_closure_readiness.py\n"
        ),
        issue_body="""
## Acceptance criteria

- Detect executable issue workflow reports.
- Include final issue, repo, gate, report, and process state.
""",
    )
    exit_code = module.main(
        [
            "--issue",
            "287",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "fix(workflow): summarize issue workflow reports",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    generated = json.loads(capsys.readouterr().out)
    report_path = (
        tmp_path
        / ".histdatacom"
        / "closure-readiness"
        / "issue-workflow-287.json"
    )
    runner.calls.clear()

    summary_exit = module.main(
        ["--summarize-report", str(report_path)],
        repo_root=tmp_path,
        runner=runner,
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert generated["schema_version"] == module.ISSUE_WORKFLOW_SCHEMA_VERSION
    assert summary_exit == 0
    assert "Issue workflow report summary" in output
    assert "Closure report summary" not in output
    assert "issue: #287 CLOSED" in output
    assert "branch: dev -> origin/dev (aligned) ahead=0 behind=0" in output
    assert "commit: fedcba9 (HEAD -> dev) test commit" in output
    assert "pre-mutation gates: pass" in output
    assert "closure: accepted" in output
    assert "issue close: closed" in output
    assert "acceptance: ready (2/2 covered, 0 missing)" in output
    assert "report paths: ready" in output
    assert "runtime/process health: clean (0)" in output
    assert "issue: not-requested" not in output
    assert "precheck:" not in output
    assert "gates: unknown" not in output
    assert runner.calls == []


def test_summarize_issue_workflow_report_json_returns_stable_payload(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Execution report summary JSON should expose stable final states."""
    module = _module()
    runner = FakeRunner(
        status_stdout=(
            " M scripts/closure_readiness.py\n"
            " M tests/unit/test_closure_readiness.py\n"
        ),
        issue_body="""
## Acceptance criteria

- Detect executable issue workflow reports.
- Emit a stable JSON summary.
""",
    )
    exit_code = module.main(
        [
            "--issue",
            "287",
            "--execute-workflow",
            "--pre-mutation-gates",
            "--commit-message",
            "fix(workflow): summarize issue workflow reports",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--commit-path",
            "tests/unit/test_closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    capsys.readouterr()
    report_path = (
        tmp_path
        / ".histdatacom"
        / "closure-readiness"
        / "issue-workflow-287.json"
    )
    runner.calls.clear()

    summary_exit = module.main(
        ["--summarize-report", str(report_path), "--json"],
        repo_root=tmp_path,
        runner=runner,
    )
    summary = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert summary_exit == 0
    assert summary["schema_version"] == (
        module.ISSUE_WORKFLOW_SUMMARY_SCHEMA_VERSION
    )
    assert summary["source_schema_version"] == (
        module.ISSUE_WORKFLOW_SCHEMA_VERSION
    )
    assert summary["accepted"] is True
    assert summary["readiness"]["state"] == "ready"
    assert summary["issue"]["label"] == "#287 CLOSED"
    assert summary["repo"]["branch"] == "dev"
    assert summary["repo"]["upstream"] == "origin/dev"
    assert summary["repo"]["upstream_state"] == "aligned"
    assert summary["repo"]["dirty"] is False
    assert summary["commit"]["summary"].startswith("fedcba9 ")
    assert summary["pre_mutation_gates"]["state"] == "pass"
    assert summary["acceptance_coverage"]["state"] == "ready"
    assert summary["closure"]["accepted"] is True
    assert summary["closure"]["issue_close_state"] == "closed"
    assert summary["report_paths"]["state"] == "ready"
    assert summary["process_health"]["after"]["state"] == "clean"
    assert "precheck" not in summary
    assert "gates" not in summary
    assert runner.calls == []


def test_summarize_issue_workflow_report_includes_slow_phase_summary() -> None:
    """Compact execution summaries should expose bounded slow phase evidence."""
    module = _module()
    report = {
        "schema_version": module.ISSUE_WORKFLOW_SCHEMA_VERSION,
        "issue_number": 290,
        "readiness": {"state": "ready"},
        "workflow_progress": {
            "state": "completed",
            "stream": "stderr-line",
            "phase_count": 5,
            "event_count": 10,
            "elapsed_seconds": 40.5,
            "last_completed_phase": "report-writing",
            "phases": [
                {
                    "phase": "initial-readiness",
                    "status": "completed",
                    "duration_seconds": 1.25,
                },
                {
                    "phase": "pytest",
                    "status": "completed",
                    "duration_seconds": 21.25,
                    "log_path": "/Users/example/private/pytest.log",
                },
                {
                    "phase": "pre-commit",
                    "status": "completed",
                    "duration_seconds": 12,
                },
                {
                    "phase": "push",
                    "status": "completed",
                    "duration_seconds": 3.5,
                    "command_label": "git push",
                },
                {
                    "phase": "report-writing",
                    "status": "completed",
                    "duration_seconds": 0.25,
                },
            ],
            "events": [],
        },
    }

    summary = module.summarize_issue_workflow_report(report)
    progress = summary["workflow_progress"]
    human = module.render_issue_workflow_summary_human(summary)

    assert [phase["phase"] for phase in progress["slow_phases"]] == [
        "pytest",
        "pre-commit",
        "push",
    ]
    assert progress["slow_phases"][0]["duration_seconds"] == 21.25
    assert progress["slow_phases"][2]["command_label"] == "git push"
    assert "log_path" not in progress["slow_phases"][0]
    assert progress["terminal_phase"] == {}
    assert "slow: pytest 21.250s, pre-commit 12.000s, push 3.500s" in human


def test_summarize_issue_workflow_report_shows_terminal_blocked_phase() -> None:
    """Blocked summaries should show the blocking phase and existing timings."""
    module = _module()
    report = {
        "schema_version": module.ISSUE_WORKFLOW_SCHEMA_VERSION,
        "issue_number": 290,
        "readiness": {
            "state": "blocked",
            "blocking_checks": ["pre-mutation-gates"],
        },
        "workflow_progress": {
            "state": "blocked",
            "stream": "stderr-line",
            "phase_count": 3,
            "event_count": 5,
            "elapsed_seconds": 5.75,
            "last_completed_phase": "initial-readiness",
            "phases": [
                {
                    "phase": "initial-readiness",
                    "status": "completed",
                    "duration_seconds": 4.0,
                },
                {
                    "phase": "pre-mutation-gates",
                    "status": "blocked",
                    "duration_seconds": 1.5,
                    "message": "pre-commit failed",
                },
                {
                    "phase": "closure-gates",
                    "status": "skipped",
                    "message": "workflow blocked",
                },
            ],
            "events": [],
        },
    }

    summary = module.summarize_issue_workflow_report(report)
    progress = summary["workflow_progress"]
    human = module.render_issue_workflow_summary_human(summary)

    assert progress["terminal_phase"]["phase"] == "pre-mutation-gates"
    assert progress["terminal_phase"]["status"] == "blocked"
    assert progress["terminal_phase"]["message"] == "pre-commit failed"
    assert [phase["phase"] for phase in progress["slow_phases"]] == [
        "initial-readiness",
        "pre-mutation-gates",
    ]
    assert "blocked: pre-mutation-gates 1.500s" in human
    assert (
        "slow: initial-readiness 4.000s, " "pre-mutation-gates blocked 1.500s"
    ) in human


def test_summarize_issue_workflow_report_shows_terminal_failed_phase() -> None:
    """Failed summaries should show the failed phase even when it is not slow."""
    module = _module()
    report = {
        "schema_version": module.ISSUE_WORKFLOW_SCHEMA_VERSION,
        "issue_number": 290,
        "readiness": {"state": "blocked", "blocking_checks": ["commit"]},
        "workflow_progress": {
            "state": "failed",
            "stream": "stderr-line",
            "phase_count": 2,
            "event_count": 4,
            "elapsed_seconds": 30.2,
            "last_completed_phase": "pre-mutation-gates",
            "phases": [
                {
                    "phase": "pre-mutation-gates",
                    "status": "completed",
                    "duration_seconds": 29.5,
                },
                {
                    "phase": "commit",
                    "status": "failed",
                    "duration_seconds": 0.2,
                    "message": "commit failed",
                },
            ],
            "events": [],
        },
    }

    summary = module.summarize_issue_workflow_report(report)
    progress = summary["workflow_progress"]
    human = module.render_issue_workflow_summary_human(summary)

    assert progress["terminal_phase"]["phase"] == "commit"
    assert progress["terminal_phase"]["status"] == "failed"
    assert [phase["phase"] for phase in progress["slow_phases"]] == [
        "pre-mutation-gates",
        "commit",
    ]
    assert "failed: commit 0.200s" in human
    assert "commit failed 0.200s" in human


def test_summarize_issue_workflow_report_routes_markdown_and_close_comment(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Saved execution reports should reuse execution markdown and comments."""
    module = _module()
    runner = FakeRunner(
        status_stdout=" M scripts/closure_readiness.py\n",
        issue_body="""
## Acceptance criteria

- Route execution report output modes by schema.
""",
    )
    exit_code = module.main(
        [
            "--issue",
            "287",
            "--execute-workflow",
            "--commit-message",
            "fix(workflow): summarize issue workflow reports",
            "--commit-path",
            "scripts/closure_readiness.py",
            "--full-json",
        ],
        repo_root=tmp_path,
        runner=runner,
    )
    capsys.readouterr()
    report_path = (
        tmp_path
        / ".histdatacom"
        / "closure-readiness"
        / "issue-workflow-287.json"
    )
    runner.calls.clear()

    markdown_exit = module.main(
        ["--summarize-report", str(report_path), "--markdown"],
        repo_root=tmp_path,
        runner=runner,
    )
    markdown = capsys.readouterr().out
    close_exit = module.main(
        ["--summarize-report", str(report_path), "--print-close-comment"],
        repo_root=tmp_path,
        runner=runner,
    )
    close_comment = capsys.readouterr().out

    assert exit_code == 0
    assert markdown_exit == 0
    assert close_exit == 0
    assert "# Issue Workflow Execution" in markdown
    assert "# Closure Readiness Report" not in markdown
    assert "Closure readiness: ready" in close_comment
    assert "Issue: #287 OPEN" in close_comment
    assert runner.calls == []


def test_report_summary_and_markdown_expose_report_path_ignore_status(
    tmp_path: Path,
    capsys: Any,
) -> None:
    """Report reuse should show whether saved outputs were gitignored."""
    module = _module()
    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=279,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=FakeRunner(),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    report = module.attach_report_paths(
        report,
        json_path=(
            tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.json"
        ),
        markdown_path=(
            tmp_path / ".histdatacom" / "closure-readiness" / "closure-279.md"
        ),
        repo_root=tmp_path,
        default_json=True,
        default_markdown=True,
        runner=FakeRunner(),
    )
    report_path = tmp_path / "closure.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    exit_code = module.main(
        ["--summarize-report", str(report_path), "--json"],
        repo_root=tmp_path,
        runner=FakeRunner(),
    )
    summary = json.loads(capsys.readouterr().out)
    markdown = module.render_markdown(report)

    assert exit_code == 0
    assert summary["report_paths"]["state"] == "ready"
    assert summary["report_paths"]["outputs"]["json"]["gitignore_state"] == (
        "ignored"
    )
    assert "| json | `.histdatacom/closure-readiness/closure-279.json`" in (
        markdown
    )
    assert "| markdown | `.histdatacom/closure-readiness/closure-279.md`" in (
        markdown
    )
    assert "ignored | ignored | will write" in markdown


def test_markdown_contains_pasteable_close_comment(tmp_path: Path) -> None:
    """Markdown output should include a GitHub-ready evidence block."""
    module = _module()
    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=274,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=FakeRunner(),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    markdown = module.render_markdown(report)

    assert "# Closure Readiness Report" in markdown
    assert "## GitHub Close Comment" in markdown
    assert "Issue: #274 OPEN" in markdown
    assert "Gates: readme-help-sync=pass" in markdown


def test_markdown_includes_issue_close_action(tmp_path: Path) -> None:
    """Markdown evidence should include close action readback when present."""
    module = _module()
    runner = FakeRunner()
    report = module.build_readiness_report(
        repo_root=tmp_path,
        issue=278,
        run_gates=True,
        artifact_roots=(tmp_path / "data",),
        process_rows=(),
        runner=runner,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    report = module.attach_issue_close_action(
        report,
        repo_root=tmp_path,
        runner=runner,
    )

    markdown = module.render_markdown(report)

    assert "## GitHub Close Action" in markdown
    assert "- State: closed" in markdown
    assert "- Final issue: #278 CLOSED" in markdown


def _completed(
    args: Sequence[str],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        list(args),
        returncode,
        stdout=stdout,
        stderr=stderr,
    )

"""Tests for the local closure-readiness helper."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType
from typing import Any, Sequence


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
        precommit_returncode: int = 0,
        precommit_changes: str = "",
        release_returncode: int = 0,
        ps_outputs: Sequence[str] = ("",),
        issue_state: str = "OPEN",
        close_returncode: int = 0,
    ) -> None:
        self.branch = branch
        self.status_stdout = status_stdout
        self.upstream_counts = upstream_counts
        self.precommit_returncode = precommit_returncode
        self.precommit_changes = precommit_changes
        self.release_returncode = release_returncode
        self.ps_outputs = tuple(ps_outputs)
        self.issue_state = issue_state
        self.close_returncode = close_returncode
        self.calls: list[tuple[str, ...]] = []
        self.status_calls = 0
        self.ps_calls = 0

    def __call__(
        self,
        command: Sequence[str],
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        args = tuple(command)
        self.calls.append(args)
        if args == ("git", "rev-parse", "--abbrev-ref", "HEAD"):
            return _completed(args, stdout=f"{self.branch}\n")
        if args == ("git", "rev-parse", "HEAD"):
            return _completed(
                args,
                stdout="abcdef1234567890abcdef1234567890abcdef12\n",
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
        if args == ("git", "status", "--porcelain=v1", "--untracked-files=all"):
            self.status_calls += 1
            if self.precommit_changes and self.status_calls >= 11:
                return _completed(args, stdout=self.precommit_changes)
            return _completed(args, stdout=self.status_stdout)
        if (
            len(args) == 6
            and args[:3] == ("gh", "issue", "view")
            and args[4:] == ("--json", "number,state,title,url")
        ):
            number = int(args[3])
            return _completed(
                args,
                stdout=json.dumps(
                    {
                        "number": number,
                        "state": self.issue_state,
                        "title": "chore(v1.4.0): helper",
                        "url": f"https://github.com/example/repo/issues/{number}",
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
            return _completed(
                args,
                returncode=self.precommit_returncode,
                stdout=(
                    "architecture-diagrams failed\n"
                    if self.precommit_returncode
                    else "all hooks passed\n"
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
    assert payload["report_paths"]["markdown"]["default"] is True


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

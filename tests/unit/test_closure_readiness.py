"""Tests for the local closure-readiness helper."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType
from typing import Sequence


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
        precommit_returncode: int = 0,
        precommit_changes: str = "",
        release_returncode: int = 0,
        ps_outputs: Sequence[str] = ("",),
    ) -> None:
        self.precommit_returncode = precommit_returncode
        self.precommit_changes = precommit_changes
        self.release_returncode = release_returncode
        self.ps_outputs = tuple(ps_outputs)
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
            return _completed(args, stdout="dev\n")
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
            return _completed(args, stdout="0\t0\n")
        if args == ("git", "status", "--porcelain=v1", "--untracked-files=all"):
            self.status_calls += 1
            if self.precommit_changes and self.status_calls >= 11:
                return _completed(args, stdout=self.precommit_changes)
            return _completed(args)
        if args == (
            "gh",
            "issue",
            "view",
            "274",
            "--json",
            "number,state,title,url",
        ):
            return _completed(
                args,
                stdout=json.dumps(
                    {
                        "number": 274,
                        "state": "OPEN",
                        "title": "chore(v1.4.0): helper",
                        "url": "https://github.com/example/repo/issues/274",
                    }
                ),
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

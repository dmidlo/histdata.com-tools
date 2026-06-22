"""Tests for local developer tool hook launchers."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_DEV_TOOL = REPO_ROOT / "scripts" / "run_dev_tool.py"


def test_run_dev_tool_resolves_venv_tool_without_ambient_path() -> None:
    """The hook launcher should not require venv/bin on ambient PATH."""
    env = os.environ.copy()
    env["HISTDATACOM_DEV_VENV"] = sys.prefix
    env["PATH"] = os.defpath
    result = subprocess.run(  # noqa:S603
        [sys.executable, str(RUN_DEV_TOOL), "coverage", "--version"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0
    assert "Coverage.py" in result.stdout


def test_run_dev_tool_remove_is_cross_platform(tmp_path: Path) -> None:
    """Coverage cleanup should not depend on a platform-specific rm binary."""
    artifact = tmp_path / ".coverage"
    artifact.write_text("coverage data", encoding="UTF-8")

    result = subprocess.run(  # noqa:S603
        [sys.executable, str(RUN_DEV_TOOL), "--remove", str(artifact)],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0
    assert not artifact.exists()


def test_local_pre_commit_hooks_use_dev_tool_launcher() -> None:
    """Local system hooks should resolve tools through the repo launcher."""
    config = yaml.safe_load((REPO_ROOT / ".pre-commit-config.yaml").read_text())
    [local_repo] = [
        repo for repo in config["repos"] if repo.get("repo") == "local"
    ]
    hooks = {hook["id"]: hook for hook in local_repo["hooks"]}

    assert hooks["histdatacom"]["entry"] == (
        "scripts/run_dev_tool.py histdatacom"
    )
    for hook_id in ("coverage-run", "coverage-combine", "coverage-report"):
        assert hooks[hook_id]["entry"] == "scripts/run_dev_tool.py coverage"
    assert hooks["coverage-rm"]["entry"] == "scripts/run_dev_tool.py"
    assert hooks["coverage-rm"]["args"] == ["--remove", ".coverage"]

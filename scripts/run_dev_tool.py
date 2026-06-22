#!/usr/bin/env python3
"""Run developer tools from the project virtual environment.

Git hooks are executed by Git, not by an activated shell.  This launcher keeps
local hooks tied to the repository virtual environment without requiring
callers to prefix every git command with a venv-specific PATH.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
VENV_ENV_VAR = "HISTDATACOM_DEV_VENV"


def _candidate_virtualenvs() -> tuple[Path, ...]:
    """Return virtualenv candidates in precedence order."""
    candidates: list[Path] = []
    for env_var in (VENV_ENV_VAR, "VIRTUAL_ENV"):
        value = os.environ.get(env_var)
        if value:
            candidates.append(Path(value).expanduser())
    candidates.extend((REPO_ROOT / "venv", REPO_ROOT / ".venv"))
    return tuple(dict.fromkeys(candidates))


def _venv_bin_dirs(venv_dir: Path) -> tuple[Path, ...]:
    """Return possible script directories for a virtualenv."""
    return (venv_dir / "bin", venv_dir / "Scripts")


def _tool_candidates(tool_name: str, bin_dir: Path) -> tuple[Path, ...]:
    """Return possible executable names for a virtualenv tool."""
    names = (tool_name, f"{tool_name}.exe", f"{tool_name}.cmd")
    return tuple(bin_dir / name for name in names)


def _find_tool(tool_name: str) -> Path:
    """Find a tool executable under the project virtual environment."""
    searched: list[Path] = []
    for venv_dir in _candidate_virtualenvs():
        for bin_dir in _venv_bin_dirs(venv_dir):
            for candidate in _tool_candidates(tool_name, bin_dir):
                searched.append(candidate)
                if candidate.is_file():
                    return candidate
    search_list = "\n  - ".join(str(path) for path in searched)
    raise SystemExit(
        f"Unable to find {tool_name!r} in the project virtual environment.\n"
        "Create/install the dev environment first:\n"
        "  python -m venv venv\n"
        "  source venv/bin/activate  # Windows: .\\venv\\Scripts\\Activate.ps1\n"
        '  PYTHONNOUSERSITE=1 python -m pip install -e ".[dev]"\n'
        f"Checked:\n  - {search_list}"
    )


def _remove_files(paths: list[str]) -> int:
    """Remove files relative to the repository root if present."""
    for path_value in paths:
        path = Path(path_value)
        if not path.is_absolute():
            path = REPO_ROOT / path
        try:
            path.unlink()
        except FileNotFoundError:
            continue
    return 0


def _run_tool(tool_name: str, tool_args: list[str]) -> int:
    """Run a virtualenv tool with the venv script directory on PATH."""
    executable = _find_tool(tool_name)
    env = os.environ.copy()
    env["PYTHONNOUSERSITE"] = "1"
    env["PATH"] = os.pathsep.join(
        (
            str(executable.parent),
            env.get("PATH", ""),
        )
    )
    return subprocess.call(  # noqa:S603
        [str(executable), *tool_args],
        cwd=REPO_ROOT,
        env=env,
    )


def main(argv: list[str] | None = None) -> int:
    """Run a developer tool or maintenance command."""
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise SystemExit("Usage: run_dev_tool.py TOOL [ARGS...]")
    if args[0] == "--remove":
        return _remove_files(args[1:])
    return _run_tool(args[0], args[1:])


if __name__ == "__main__":
    raise SystemExit(main())

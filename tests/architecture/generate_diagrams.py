# type: ignore
"""Generate deterministic architecture diagrams committed with the repo."""

from __future__ import annotations

import sys
from pathlib import Path

import sh

project_dir = Path(".")
package_dir = project_dir / "src" / "histdatacom"
base_dir = project_dir / Path("tests", "architecture")


def _tool_command(name: str) -> sh.Command:
    """Return a Python script command from the active virtual environment."""
    bin_dir = Path(sys.executable).parent
    for suffix in ("", ".exe", ".cmd"):
        candidate = bin_dir / f"{name}{suffix}"
        if candidate.is_file():
            return sh.Command(str(candidate))
    return sh.Command(name)


def generate_pyreverse_svgs() -> None:
    """Generate deterministic package and class architecture SVGs."""
    pyreverse = _tool_command("pyreverse")
    pyreverse(
        "-f",
        "ALL",
        "-o",
        "svg",
        "--project",
        "pyreverse",
        "--output-directory",
        base_dir.resolve(),
        package_dir.resolve(),
    )


def main() -> None:
    """Generate architecture diagrams."""
    generate_pyreverse_svgs()


if __name__ == "__main__":
    main()

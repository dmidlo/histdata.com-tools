"""Tests for README CLI help synchronization."""

from __future__ import annotations

from pathlib import Path

from histdatacom.readme_help import (
    README_HELP_COMMAND,
    extract_main_help,
    generated_main_help,
    main,
    sync_main_help,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
README_PATH = REPO_ROOT / "README.md"


def test_readme_main_help_excerpt_matches_parser_help() -> None:
    """README main help should stay synchronized with ArgParser output."""
    readme = README_PATH.read_text(encoding="utf-8")

    assert extract_main_help(readme) == generated_main_help()


def test_sync_main_help_replaces_only_help_excerpt() -> None:
    """The sync helper should update the generated block in place."""
    stale_help = "usage: stale\n"
    expected_help = "usage: current\n\noptions:\n  -h, --help\n"
    markdown = (
        "# Example\n\n"
        "```txt\n"
        f"{README_HELP_COMMAND}\n"
        "```\n\n"
        "```txt\n"
        f"{stale_help}"
        "```\n\n"
        "Afterward.\n"
    )

    updated, changed = sync_main_help(markdown, expected_help)

    assert changed is True
    assert stale_help not in updated
    assert expected_help.rstrip("\n") in updated
    assert updated.endswith("Afterward.\n")


def test_sync_main_help_reports_unchanged_when_current() -> None:
    """The sync helper should not rewrite already-current README content."""
    expected_help = "usage: current\n\noptions:\n  -h, --help\n"
    markdown = (
        "```txt\n"
        f"{README_HELP_COMMAND}\n"
        "```\n\n"
        "```txt\n"
        f"{expected_help}"
        "```\n"
    )

    updated, changed = sync_main_help(markdown, expected_help)

    assert changed is False
    assert updated == markdown


def test_sync_command_check_mode_fails_for_stale_readme(
    tmp_path: Path,
) -> None:
    """The sync command should fail without rewriting in check mode."""
    readme_path = tmp_path / "README.md"
    readme_path.write_text(
        "```txt\n"
        f"{README_HELP_COMMAND}\n"
        "```\n\n"
        "```txt\n"
        "usage: stale\n"
        "```\n",
        encoding="utf-8",
    )

    exit_code = main(["--readme", str(readme_path), "--check"])

    assert exit_code == 1
    assert "usage: stale" in readme_path.read_text(encoding="utf-8")

"""Helpers for keeping README CLI help excerpts synchronized."""

from __future__ import annotations

import argparse
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
import os
from pathlib import Path
import sys

from histdatacom import Options
from histdatacom.cli import ArgParser

DEFAULT_README_HELP_COLUMNS = 80
README_HELP_COMMAND = "histdatacom -h"
README_HELP_COMMAND_FENCE = f"```txt\n{README_HELP_COMMAND}\n```\n"
README_HELP_BLOCK_FENCE = "```txt\n"


class ReadmeHelpSyncError(RuntimeError):
    """Raised when a README help excerpt cannot be synchronized."""


def generated_main_help(
    *,
    columns: int = DEFAULT_README_HELP_COLUMNS,
) -> str:
    """Return deterministic main command help for README snapshots."""
    with _fixed_columns(columns):
        parser = ArgParser(Options())
        parser._set_args()
        return _normalized_help(parser.format_help())


def extract_main_help(markdown: str) -> str:
    """Return the README ``histdatacom -h`` help excerpt."""
    content_start, content_end = _main_help_bounds(markdown)
    return _normalized_help(markdown[content_start:content_end])


def sync_main_help(markdown: str, help_text: str) -> tuple[str, bool]:
    """Return README markdown with the main help excerpt replaced."""
    content_start, content_end = _main_help_bounds(markdown)
    expected = _normalized_help(help_text).rstrip("\n")
    current = markdown[content_start:content_end]
    if _normalized_help(current) == _normalized_help(expected):
        return markdown, False
    updated = markdown[:content_start] + expected + markdown[content_end:]
    return updated, True


def sync_readme_file(
    path: str | Path,
    *,
    check: bool = False,
    columns: int = DEFAULT_README_HELP_COLUMNS,
) -> bool:
    """Synchronize the README main help block.

    Returns ``True`` when the file already matched or was updated, and ``False``
    when ``check`` mode found stale content.
    """
    readme_path = Path(path)
    markdown = readme_path.read_text(encoding="utf-8")
    updated, changed = sync_main_help(
        markdown,
        generated_main_help(columns=columns),
    )
    if check:
        return not changed
    if changed:
        readme_path.write_text(updated, encoding="utf-8")
    return True


def main(argv: Sequence[str] | None = None) -> int:
    """Run the README CLI help synchronization command."""
    parser = argparse.ArgumentParser(
        prog="sync_readme_cli_help.py",
        description="Synchronize README CLI help excerpts with ArgParser.",
    )
    parser.add_argument(
        "--readme",
        default="README.md",
        metavar="PATH",
        help="README path to check or update; defaults to README.md",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail instead of rewriting when the README excerpt is stale",
    )
    parser.add_argument(
        "--columns",
        default=DEFAULT_README_HELP_COLUMNS,
        metavar="COUNT",
        type=_positive_int,
        help=(
            "help formatter width used for stable README snapshots; defaults "
            f"to {DEFAULT_README_HELP_COLUMNS}"
        ),
    )
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    readme_path = Path(str(args.readme))
    if sync_readme_file(
        readme_path,
        check=bool(args.check),
        columns=int(args.columns),
    ):
        action = "checked" if args.check else "synchronized"
        print(f"README CLI help {action}: {readme_path}")  # noqa:T201
        return 0
    print(  # noqa:T201
        "README CLI help is stale. Run "
        "`python scripts/sync_readme_cli_help.py` to update it."
    )
    return 1


def _main_help_bounds(markdown: str) -> tuple[int, int]:
    command_index = markdown.find(README_HELP_COMMAND_FENCE)
    if command_index < 0:
        raise ReadmeHelpSyncError(
            f"README command block for {README_HELP_COMMAND!r} was not found"
        )
    fence_start = markdown.find(
        README_HELP_BLOCK_FENCE,
        command_index + len(README_HELP_COMMAND_FENCE),
    )
    if fence_start < 0:
        raise ReadmeHelpSyncError(
            f"README help block for {README_HELP_COMMAND!r} was not found"
        )
    content_start = fence_start + len(README_HELP_BLOCK_FENCE)
    content_end = markdown.find("\n```", content_start)
    if content_end < 0:
        raise ReadmeHelpSyncError(
            f"README help block for {README_HELP_COMMAND!r} is not closed"
        )
    return content_start, content_end


def _normalized_help(value: str) -> str:
    return value.replace("\r\n", "\n").rstrip("\n") + "\n"


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not an integer"
        ) from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


@contextmanager
def _fixed_columns(columns: int) -> Iterator[None]:
    previous = os.environ.get("COLUMNS")
    os.environ["COLUMNS"] = str(columns)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("COLUMNS", None)
        else:
            os.environ["COLUMNS"] = previous

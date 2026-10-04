"""Stdlib-only refusal boundary for legacy mutations of managed evidence.

This is cooperative package safety, not a filesystem sandbox. In particular,
privileged or uncoordinated concurrent writers can defeat a pathname preflight.
Managed-store collection has its own locked, descriptor-relative protocol.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
from collections.abc import Iterable, Sequence

MANAGED_ARTIFACT_MARKER = ".histdatacom-retention.json"
MAX_BOUNDARY_PATHS = 1024
MAX_BOUNDARY_ENTRIES = 65_536
MAX_BOUNDARY_DEPTH = 128
MAX_BOUNDARY_PATH_BYTES = 4096


class ManagedArtifactBoundaryError(ValueError):
    """Legacy mutation cannot establish an unmanaged target boundary."""


def _path(value: str | os.PathLike[str]) -> Path:
    try:
        text = os.fspath(value)
        if (
            not isinstance(text, str)
            or not text
            or "\x00" in text
            or len(text) > MAX_BOUNDARY_PATH_BYTES
            or len(text.encode("utf-8")) > MAX_BOUNDARY_PATH_BYTES
        ):
            raise ValueError
        return Path(text).expanduser().absolute()
    except (OSError, RuntimeError, TypeError, ValueError, UnicodeError):
        raise ManagedArtifactBoundaryError("invalid mutation path") from None


def _marker_present(directory: Path) -> bool:
    try:
        (directory / MANAGED_ARTIFACT_MARKER).lstat()
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError:
        raise ManagedArtifactBoundaryError(
            "managed-marker absence cannot be established"
        ) from None
    return True


def _ancestors(path: Path) -> None:
    try:
        resolved = path.resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        raise ManagedArtifactBoundaryError(
            "mutation path cannot be resolved safely"
        ) from None
    for candidate in (path, resolved):
        for directory in (candidate, *candidate.parents):
            if directory.name == MANAGED_ARTIFACT_MARKER or _marker_present(
                directory
            ):
                raise ManagedArtifactBoundaryError(
                    "legacy mutation refused for managed artifact namespace"
                )


def _details(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        raise ManagedArtifactBoundaryError(
            "mutation target cannot be inspected"
        ) from None


def _tree(path: Path) -> tuple[tuple[Path, os.stat_result], ...]:
    """Inspect a bounded complete tree without following directory links."""
    found: list[tuple[Path, os.stat_result]] = []
    pending = [(path, 0)]
    while pending:
        current, depth = pending.pop()
        if depth > MAX_BOUNDARY_DEPTH:
            raise ManagedArtifactBoundaryError("mutation tree exceeds depth")
        _ancestors(current)
        details = _details(current)
        if details is None:
            if current != path:
                raise ManagedArtifactBoundaryError(
                    "mutation tree changed during inspection"
                )
            continue
        if len(found) >= MAX_BOUNDARY_ENTRIES:
            raise ManagedArtifactBoundaryError("mutation tree exceeds bound")
        found.append((current, details))
        if not stat.S_ISDIR(details.st_mode):
            continue
        try:
            with os.scandir(current) as entries:
                children: list[Path] = []
                for entry in entries:
                    if len(found) + len(pending) + len(children) >= (
                        MAX_BOUNDARY_ENTRIES
                    ):
                        raise ManagedArtifactBoundaryError(
                            "mutation tree exceeds bound"
                        )
                    children.append(Path(entry.path))
        except OSError:
            raise ManagedArtifactBoundaryError(
                "mutation tree cannot be completely inspected"
            ) from None
        pending.extend((child, depth + 1) for child in sorted(children))
    return tuple(found)


def assert_unmanaged_mutation_paths(
    paths: Iterable[str | os.PathLike[str]], *, recursive: bool = False
) -> None:
    """Preflight every target before a legacy operation changes any of them.

    Marker presence suffices: malformed bytes, directories and dangling links
    are protected too. Recursive operations must request descendant scanning.
    There is deliberately no cached admission or privileged bypass flag.
    """
    if isinstance(paths, (str, bytes, os.PathLike)):
        raise ManagedArtifactBoundaryError("mutation paths require iterable")
    admitted: list[Path] = []
    for value in paths:
        if len(admitted) >= MAX_BOUNDARY_PATHS:
            raise ManagedArtifactBoundaryError("too many mutation paths")
        admitted.append(_path(value))
    total = 0
    for path in admitted:
        _ancestors(path)
        if recursive:
            total += len(_tree(path))
            if total > MAX_BOUNDARY_ENTRIES:
                raise ManagedArtifactBoundaryError(
                    "mutation inventory exceeds bound"
                )


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def guarded_campaign_cleanup(
    targets: Sequence[str | os.PathLike[str]], *, mode: str
) -> dict[str, int | str]:
    """Execute an explicit legacy campaign cleanup after whole-set preflight.

    This is not managed GC and grants no retention classification. All actual
    removals are individual files or already-empty directories, never rmtree.
    An I/O error propagates; partial effects are not reported as success.
    """
    if mode not in {"cache", "working-artifacts"}:
        raise ValueError("unsupported campaign cleanup mode")
    if not targets or len(targets) > MAX_BOUNDARY_PATHS:
        raise ManagedArtifactBoundaryError("invalid cleanup target count")
    roots = tuple(_path(value) for value in targets)
    resolved_roots = tuple(path.resolve(strict=False) for path in roots)
    for index, path in enumerate(resolved_roots):
        if any(
            path == other
            or path.is_relative_to(other)
            or other.is_relative_to(path)
            for other in resolved_roots[:index]
        ):
            raise ManagedArtifactBoundaryError("overlapping cleanup targets")
    for path in roots:
        resolved = path.resolve(strict=False)
        if resolved in {Path(resolved.anchor), Path.home().resolve()} or (
            (resolved / ".git").exists()
        ):
            raise ManagedArtifactBoundaryError("unsafe cleanup root")
    assert_unmanaged_mutation_paths(roots, recursive=True)
    inventory: dict[Path, os.stat_result] = {}
    for root in roots:
        for path, details in _tree(root):
            inventory[path] = details
            if len(inventory) > MAX_BOUNDARY_ENTRIES:
                raise ManagedArtifactBoundaryError(
                    "cleanup inventory exceeds bound"
                )
    # Refuse unsupported objects before the first effect, including in a later
    # supplied target. Do not follow a link in order to classify its payload.
    for details in inventory.values():
        if not (
            stat.S_ISREG(details.st_mode)
            or stat.S_ISDIR(details.st_mode)
            or stat.S_ISLNK(details.st_mode)
        ):
            raise ManagedArtifactBoundaryError("unsupported cleanup object")
    removed_files = removed_directories = removed_bytes = 0
    ordered = sorted(inventory, key=lambda path: (-len(path.parts), str(path)))
    for path in ordered:
        previous = inventory[path]
        directory = stat.S_ISDIR(previous.st_mode)
        if mode == "cache" and not (
            stat.S_ISREG(previous.st_mode) and path.name == ".data"
        ):
            continue
        assert_unmanaged_mutation_paths((path,))
        current = path.lstat()
        if directory:
            # Removing children changes directory timestamps/size, not identity.
            if (current.st_dev, current.st_ino, current.st_mode) != (
                previous.st_dev,
                previous.st_ino,
                previous.st_mode,
            ):
                raise ManagedArtifactBoundaryError("cleanup directory changed")
            path.rmdir()
            removed_directories += 1
        else:
            if _identity(current) != _identity(previous):
                raise ManagedArtifactBoundaryError("cleanup object changed")
            path.unlink()
            removed_files += 1
            removed_bytes += previous.st_size
    return {
        "mode": mode,
        "removed_files": removed_files,
        "removed_directories": removed_directories,
        "removed_bytes": removed_bytes,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Run guarded cleanup when a previously generated campaign executes."""
    parser = argparse.ArgumentParser(prog="managed-artifact-boundary")
    parser.add_argument(
        "--cleanup-mode", required=True, choices=("cache", "working-artifacts")
    )
    parser.add_argument("targets", nargs="+")
    args = parser.parse_args(argv)
    try:
        result = guarded_campaign_cleanup(args.targets, mode=args.cleanup_mode)
    except (OSError, ValueError) as error:
        parser.exit(1, f"cleanup refused: {error}\n")
    print(json.dumps(result, sort_keys=True))  # noqa:T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Cleanup helpers for transient downloaded source artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

TRANSIENT_SOURCE_SUFFIXES = (".zip", ".csv", ".xls", ".xlsx")


@dataclass(frozen=True, slots=True)
class SourceCleanupResult:
    """Summary of a transient source-artifact cleanup scan."""

    root: str
    dry_run: bool
    suffixes: tuple[str, ...]
    matched_count: int
    matched_size_bytes: int
    deleted_count: int
    deleted_size_bytes: int
    by_suffix: dict[str, dict[str, int]] = field(default_factory=dict)
    errors: tuple[dict[str, str], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable payload."""
        return {
            "root": self.root,
            "dry_run": self.dry_run,
            "suffixes": list(self.suffixes),
            "matched_count": self.matched_count,
            "matched_size_bytes": self.matched_size_bytes,
            "deleted_count": self.deleted_count,
            "deleted_size_bytes": self.deleted_size_bytes,
            "by_suffix": self.by_suffix,
            "errors": list(self.errors),
        }


def find_transient_source_artifacts(
    root: str | Path,
    *,
    suffixes: Iterable[str] = TRANSIENT_SOURCE_SUFFIXES,
) -> tuple[Path, ...]:
    """Return source artifacts below ``root`` that can be safely regenerated."""
    root_path = Path(root).expanduser()
    if not root_path.exists():
        return ()
    normalized_suffixes = _normalized_suffixes(suffixes)
    return tuple(
        sorted(
            (
                path
                for path in root_path.rglob("*")
                if path.is_file() and path.suffix.lower() in normalized_suffixes
            ),
            key=lambda item: str(item),
        )
    )


def cleanup_transient_source_artifacts(
    root: str | Path,
    *,
    apply: bool = False,  # pylint: disable=redefined-builtin
    suffixes: Iterable[str] = TRANSIENT_SOURCE_SUFFIXES,
) -> SourceCleanupResult:
    """Inspect or delete transient source artifacts while preserving caches."""
    root_path = Path(root).expanduser()
    normalized_suffixes = _normalized_suffixes(suffixes)
    by_suffix = _empty_suffix_counts(normalized_suffixes)
    errors: list[dict[str, str]] = []
    matched_count = 0
    matched_size_bytes = 0
    deleted_count = 0
    deleted_size_bytes = 0

    for path in find_transient_source_artifacts(
        root_path, suffixes=normalized_suffixes
    ):
        suffix = path.suffix.lower()
        matched_count += 1
        by_suffix[suffix]["matched_count"] += 1
        try:
            size_bytes = path.stat().st_size
        except OSError as exc:
            size_bytes = 0
            errors.append(
                {
                    "path": str(path),
                    "operation": "stat",
                    "message": str(exc),
                }
            )
        matched_size_bytes += size_bytes
        by_suffix[suffix]["matched_size_bytes"] += size_bytes

        if not apply:
            continue
        try:
            path.unlink()
        except OSError as exc:
            errors.append(
                {
                    "path": str(path),
                    "operation": "delete",
                    "message": str(exc),
                }
            )
            continue
        deleted_count += 1
        deleted_size_bytes += size_bytes
        by_suffix[suffix]["deleted_count"] += 1
        by_suffix[suffix]["deleted_size_bytes"] += size_bytes

    return SourceCleanupResult(
        root=str(root_path.resolve(strict=False)),
        dry_run=not apply,
        suffixes=normalized_suffixes,
        matched_count=matched_count,
        matched_size_bytes=matched_size_bytes,
        deleted_count=deleted_count,
        deleted_size_bytes=deleted_size_bytes,
        by_suffix=by_suffix,
        errors=tuple(errors),
    )


def _normalized_suffixes(suffixes: Iterable[str]) -> tuple[str, ...]:
    normalized = {
        suffix.lower() if suffix.startswith(".") else f".{suffix.lower()}"
        for suffix in suffixes
    }
    return tuple(sorted(normalized))


def _empty_suffix_counts(suffixes: Iterable[str]) -> dict[str, dict[str, int]]:
    return {
        suffix: {
            "matched_count": 0,
            "matched_size_bytes": 0,
            "deleted_count": 0,
            "deleted_size_bytes": 0,
        }
        for suffix in suffixes
    }

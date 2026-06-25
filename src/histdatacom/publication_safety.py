"""Publication-safe serialization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath
import re
from typing import cast

from histdatacom.runtime_contracts import JSONValue

_PATHISH_KEY_SUFFIXES = (
    "_dir",
    "_dirs",
    "_directory",
    "_directories",
    "_path",
    "_paths",
    "_root",
    "_roots",
)
_PATHISH_KEYS = {
    "data_dir",
    "data_directory",
    "directory",
    "path",
    "paths",
    "reports_directory",
    "root",
    "roots",
    "store_path",
    "store_root",
}
_PUBLIC_PATH_ANCHORS = (
    "data",
    "docs",
    "tests",
    "fixtures",
    "quality-fixtures",
    ".quality",
    "reports",
    ".histdatacom",
    "manifests",
)
_LOCAL_PATH_MARKERS = (
    "/Users/",
    "/home/",
    "/private/",
    "/tmp/",
    "/var/folders/",
    "C:\\Users\\",
    "file:///",
)
_URL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")
_EMBEDDED_LOCAL_PATH_RE = re.compile(
    r"(file:///[^\s\"'<>]+|/[Uu]sers/[^\s\"'<>]+|/home/[^\s\"'<>]+|"
    r"/private/[^\s\"'<>]+|/tmp/[^\s\"'<>]+|"
    r"/var/folders/[^\s\"'<>]+|[A-Za-z]:\\Users\\[^\s\"'<>]+)"
)


def publish_safe_path(value: str) -> str:
    """Return a publication-safe relative display path."""
    text = str(value or "").strip()
    if not text:
        return ""
    if _URL_RE.match(text) and not text.startswith("file://"):
        return text

    normalized = text.removeprefix("file://").replace("\\", "/")
    normalized = _WINDOWS_DRIVE_RE.sub("", normalized)
    parts = [
        part
        for part in PurePosixPath(normalized).parts
        if part not in {"", "/"}
    ]
    if not parts:
        return ""

    for anchor in _PUBLIC_PATH_ANCHORS:
        if anchor not in parts:
            continue
        index = parts.index(anchor)
        return "/".join(parts[index:])

    if parts[0] in {"tmp", "private", "var", "Users", "home"}:
        return parts[-1]
    return "/".join(parts[-4:])


def publish_safe_json_value(
    value: JSONValue,
    *,
    key: str = "",
) -> JSONValue:
    """Return JSON with local machine paths converted to public-safe paths."""
    if isinstance(value, dict):
        return {
            str(item_key): publish_safe_json_value(
                item_value,
                key=str(item_key),
            )
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [publish_safe_json_value(item, key=key) for item in value]
    if isinstance(value, str):
        return _publish_safe_string(value, key=key)
    return value


def publish_safe_json_mapping(
    payload: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    """Return a publication-safe JSON mapping."""
    safe = publish_safe_json_value(dict(payload))
    return cast(dict[str, JSONValue], safe)


def _publish_safe_string(value: str, *, key: str) -> str:
    if _is_pathish_key(key):
        return publish_safe_path(value)
    if not any(marker in value for marker in _LOCAL_PATH_MARKERS):
        return value
    return _EMBEDDED_LOCAL_PATH_RE.sub(
        lambda match: publish_safe_path(match.group(0)),
        value,
    )


def _is_pathish_key(key: str) -> bool:
    normalized = key.strip().lower()
    return normalized in _PATHISH_KEYS or normalized.endswith(
        _PATHISH_KEY_SUFFIXES
    )

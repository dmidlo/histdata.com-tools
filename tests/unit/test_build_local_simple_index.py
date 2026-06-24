"""Tests for the local TestPyPI-style simple-index helper."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def _module() -> ModuleType:
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "build_local_simple_index.py"
    )
    spec = importlib.util.spec_from_file_location(
        "build_local_simple_index",
        script_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_simple_index_copies_release_artifacts(tmp_path: Path) -> None:
    """Local release preflight should install from a real simple index."""
    module = _module()
    dist_dir = tmp_path / "dist"
    output_root = tmp_path / "index"
    dist_dir.mkdir()
    wheel = dist_dir / "histdatacom-0.79.0-py3-none-any.whl"
    sdist = dist_dir / "histdatacom-0.79.0.tar.gz"
    wheel.write_bytes(b"wheel")
    sdist.write_bytes(b"sdist")

    report = module.build_simple_index(
        dist_dir=dist_dir,
        output_root=output_root,
    )

    package_dir = output_root / "simple" / "histdatacom"
    assert (package_dir / wheel.name).read_bytes() == b"wheel"
    assert (package_dir / sdist.name).read_bytes() == b"sdist"
    package_index = (package_dir / "index.html").read_text(encoding="utf-8")
    assert wheel.name in package_index
    assert sdist.name in package_index
    assert report["index_url"] == f"{(output_root / 'simple').as_uri()}/"
    assert sorted(Path(path).name for path in report["artifacts"]) == sorted(
        [sdist.name, wheel.name]
    )


def test_build_simple_index_rejects_missing_artifacts(tmp_path: Path) -> None:
    """A preflight with no dist artifacts should fail before pip runs."""
    module = _module()
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()

    with pytest.raises(
        SystemExit, match="no histdatacom distribution artifacts"
    ):
        module.build_simple_index(
            dist_dir=dist_dir,
            output_root=tmp_path / "index",
        )

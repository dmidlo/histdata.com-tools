"""Tests for release workflow platform-wheel coverage."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import yaml


def _load_fetch_script() -> ModuleType:
    """Load the Temporal CLI fetch helper as a test module."""
    script_path = (
        Path(__file__).resolve().parents[2] / "scripts/fetch_temporal_cli.py"
    )
    spec = importlib.util.spec_from_file_location(
        "fetch_temporal_cli",
        script_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["fetch_temporal_cli"] = module
    spec.loader.exec_module(module)
    return module


def _release_workflow() -> dict[str, object]:
    """Return the parsed release workflow YAML."""
    workflow_path = (
        Path(__file__).resolve().parents[2] / ".github/workflows/release.yml"
    )
    loaded = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_release_workflow_builds_and_smokes_all_platform_wheels() -> None:
    """Release CI should build and smoke every bundled sidecar platform."""
    workflow = _release_workflow()
    fetch_script = _load_fetch_script()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    expected_platforms = set(fetch_script.TEMPORAL_CLI_ASSETS)

    env = workflow["env"]
    assert isinstance(env, dict)
    assert env["TEMPORAL_CLI_VERSION"] == (
        fetch_script.DEFAULT_TEMPORAL_CLI_VERSION
    )

    build_platform = jobs["build-platform-wheels"]
    assert isinstance(build_platform, dict)
    build_strategy = build_platform["strategy"]
    assert isinstance(build_strategy, dict)
    build_matrix = build_strategy["matrix"]
    assert isinstance(build_matrix, dict)
    built_platforms = {
        str(item["platform_key"]) for item in build_matrix["include"]
    }
    assert built_platforms == expected_platforms

    smoke_platform = jobs["smoke-platform-wheels"]
    assert isinstance(smoke_platform, dict)
    smoke_strategy = smoke_platform["strategy"]
    assert isinstance(smoke_strategy, dict)
    smoke_matrix = smoke_strategy["matrix"]
    assert isinstance(smoke_matrix, dict)
    smoke_runners = {
        str(item["platform_key"]): str(item["runner"])
        for item in smoke_matrix["include"]
    }
    assert set(smoke_runners) == expected_platforms
    assert smoke_runners["linux-arm64"] == "ubuntu-24.04-arm"
    assert smoke_runners["macos-x86_64"] == "macos-15-intel"
    assert smoke_runners["macos-arm64"] == "macos-15"

    assemble = jobs["assemble-release-artifacts"]
    assert isinstance(assemble, dict)
    assert set(assemble["needs"]) == {
        "build-metadata",
        "build-platform-wheels",
        "smoke-platform-wheels",
    }
    assert jobs["publish-testpypi"]["needs"] == "assemble-release-artifacts"
    assert jobs["publish-pypi"]["needs"] == "assemble-release-artifacts"

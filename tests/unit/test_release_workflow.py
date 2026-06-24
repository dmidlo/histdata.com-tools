"""Tests for release workflow platform-wheel coverage."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from setuptools.config.pyprojecttoml import read_configuration
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


def _pyproject_config() -> dict[str, object]:
    """Return parsed pyproject metadata through setuptools' TOML reader."""
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    loaded = read_configuration(pyproject_path)
    assert isinstance(loaded, dict)
    return loaded


def _project_text(relative_path: str) -> str:
    """Return repository file text for release policy assertions."""
    return (Path(__file__).resolve().parents[2] / relative_path).read_text(
        encoding="utf-8"
    )


def _step_run(job: dict[str, object], step_name: str) -> str:
    """Return the shell command for a named workflow step."""
    steps = job["steps"]
    assert isinstance(steps, list)
    for step in steps:
        assert isinstance(step, dict)
        if step.get("name") == step_name:
            run = step["run"]
            assert isinstance(run, str)
            return run
    raise AssertionError(f"missing workflow step: {step_name}")


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
    build_command = _step_run(build_platform, "Build bundled platform wheel")
    assert "--fetch-report" in build_command
    assert (
        '"release-reports/temporal-cli-${{ matrix.platform_key }}.json"'
        in build_command
    )

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
    smoke_command = _step_run(
        smoke_platform,
        "Smoke bundled sidecar install hermetically",
    )
    assert "--require-bundled-current-platform" in smoke_command
    assert "--check-executable-version" in smoke_command
    assert "--start-sidecar" in smoke_command
    assert "--hermetic-sidecar-smoke" in smoke_command
    assert "--default-routing-sidecar-smoke" in smoke_command
    assert "--live-sidecar-smoke" not in smoke_command
    assert "--live-workspace .sidecar-live-workspace" in smoke_command
    assert "--live-runtime-home .sidecar-live-runtime" in smoke_command
    assert "--live-data-dir .sidecar-live-data" in smoke_command
    assert "--live-startup-timeout 45" in smoke_command
    assert "--live-completion-timeout 240" in smoke_command
    assert "--live-stop-timeout 45" in smoke_command

    assemble = jobs["assemble-release-artifacts"]
    assert isinstance(assemble, dict)
    assert set(assemble["needs"]) == {
        "build-metadata",
        "build-platform-wheels",
        "smoke-platform-wheels",
    }
    assert jobs["publish-testpypi"]["needs"] == "assemble-release-artifacts"
    assert jobs["publish-pypi"]["needs"] == "assemble-release-artifacts"
    assert jobs["publish-testpypi"]["if"] == (
        "github.event_name == 'workflow_dispatch' && "
        "inputs.release_target == 'testpypi' && "
        "github.ref == 'refs/heads/dev'"
    )
    assert jobs["publish-pypi"]["if"] == (
        "github.event_name == 'workflow_dispatch' && "
        "inputs.release_target == 'pypi' && "
        "github.ref == 'refs/heads/main'"
    )


def test_package_metadata_advertises_platform_wheel_support() -> None:
    """PyPI metadata should match the sidecar platform wheel support matrix."""
    project = _pyproject_config()["project"]
    assert isinstance(project, dict)
    classifiers = set(project["classifiers"])

    assert {
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
    } <= classifiers


def test_local_publishing_script_enforces_branch_contract() -> None:
    """Local release uploads should map dev to TestPyPI and main to PyPI."""
    script = _project_text("pypi.sh")

    assert 'testpypi_branch="${HISTDATACOM_TESTPYPI_BRANCH:-dev}"' in script
    assert 'pypi_branch="${HISTDATACOM_PYPI_BRANCH:-main}"' in script
    assert 'prepare_release_upload "TestPyPI" "${testpypi_branch}"' in script
    assert 'prepare_release_upload "PyPI" "${pypi_branch}"' in script
    assert "HISTDATACOM_ALLOW_RELEASE_BRANCH_MISMATCH" in script
    assert "refusing release upload with uncommitted tracked changes" in script
    assert "python -m twine upload -r testpypi" in script
    assert "python -m twine upload -r pypi" in script


def test_release_docs_mark_local_publishing_as_current_path() -> None:
    """Release docs should not imply Actions deployment is active today."""
    release_docs = _project_text("RELEASE.md")

    assert (
        "Local publishing is the authoritative release path today."
        in release_docs
    )
    assert "GitHub Actions" in release_docs
    assert "publishing is future architecture" in release_docs
    assert "TestPyPI is only dispatchable from `dev`" in release_docs
    assert "PyPI is only dispatchable from `main`" in release_docs
    assert (
        "`bash pypi.sh testpypi` is guarded to run from `dev`" in release_docs
    )
    assert "`bash pypi.sh pypi` is guarded to run from `main`" in release_docs

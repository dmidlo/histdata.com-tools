"""Tests for release workflow artifact policy."""

from __future__ import annotations

import importlib.util
import re
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


def _workflow_triggers(workflow: dict[str, object]) -> dict[str, object]:
    """Return workflow triggers, accounting for YAML 1.1 boolean keys."""
    triggers = workflow.get("on", workflow.get(True))
    assert isinstance(triggers, dict)
    return triggers


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


def _step(job: dict[str, object], step_name: str) -> dict[str, object]:
    """Return the full workflow step mapping for a named workflow step."""
    steps = job["steps"]
    assert isinstance(steps, list)
    for step in steps:
        assert isinstance(step, dict)
        if step.get("name") == step_name:
            return step
    raise AssertionError(f"missing workflow step: {step_name}")


def test_release_workflow_builds_platform_wheels_only_when_opted_in() -> None:
    """Bundled runtime wheels should be an explicit build-only dry-run path."""
    workflow = _release_workflow()
    fetch_script = _load_fetch_script()
    triggers = _workflow_triggers(workflow)
    dispatch = triggers["workflow_dispatch"]
    assert isinstance(dispatch, dict)
    inputs = dispatch["inputs"]
    assert isinstance(inputs, dict)
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    expected_platforms = set(fetch_script.TEMPORAL_CLI_ASSETS)

    include_input = inputs["include_bundled_platform_wheels"]
    assert isinstance(include_input, dict)
    assert include_input["default"] is False
    assert "private/offline" in str(include_input["description"])
    assert "build-only" in str(include_input["description"])
    size_confirm = inputs["bundled_platform_wheel_size_confirmed"]
    assert isinstance(size_confirm, dict)
    assert size_confirm["default"] is False
    assert "size policy" in str(size_confirm["description"])

    env = workflow["env"]
    assert isinstance(env, dict)
    assert env["TEMPORAL_CLI_VERSION"] == (
        fetch_script.DEFAULT_TEMPORAL_CLI_VERSION
    )

    validation = jobs["validate-release-inputs"]
    assert isinstance(validation, dict)
    validation_command = _step_run(validation, "Validate bundled wheel opt-in")
    assert "include_bundled_platform_wheels" in validation_command
    assert "release_target" in validation_command
    assert "build-only" in validation_command
    assert "bundled_platform_wheel_size_confirmed" in validation_command
    assert "private/offline" in validation_command

    build_platform = jobs["build-platform-wheels"]
    assert isinstance(build_platform, dict)
    assert build_platform["needs"] == "validate-release-inputs"
    assert build_platform["if"] == (
        "github.event_name == 'workflow_dispatch' && "
        "inputs.release_target == 'build-only' && "
        "inputs.include_bundled_platform_wheels == true && "
        "inputs.bundled_platform_wheel_size_confirmed == true"
    )
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
    assert smoke_platform["timeout-minutes"] == 20
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
    assert smoke_runners["windows-x86_64"] == "windows-2022"
    diagnostic_step = next(
        step
        for step in smoke_platform["steps"]
        if step.get("name") == "Diagnose Windows runtime startup"
    )
    assert diagnostic_step["if"] == "matrix.platform_key == 'windows-x86_64'"
    diagnostic_command = _step_run(
        smoke_platform,
        "Diagnose Windows runtime startup",
    )
    assert "--skip-cli" in diagnostic_command
    assert "--windows-runtime-diagnostic" in diagnostic_command
    assert "--require-bundled-current-platform" in diagnostic_command
    assert "--check-executable-version" in diagnostic_command
    smoke_command = _step_run(
        smoke_platform,
        "Smoke bundled runtime install hermetically",
    )
    hermetic_step = next(
        step
        for step in smoke_platform["steps"]
        if step.get("name") == "Smoke bundled runtime install hermetically"
    )
    assert hermetic_step["if"] == "matrix.platform_key != 'windows-x86_64'"
    assert "--require-bundled-current-platform" in smoke_command
    assert "--check-executable-version" in smoke_command
    assert "--start-runtime" in smoke_command
    assert "--hermetic-runtime-smoke" in smoke_command
    assert "--default-routing-runtime-smoke" in smoke_command
    assert "--live-runtime-smoke" not in smoke_command
    assert "--live-workspace .runtime-live-workspace" in smoke_command
    assert "--live-runtime-home .runtime-live-home" in smoke_command
    assert "--live-data-dir .runtime-live-data" in smoke_command
    assert "--live-startup-timeout 45" in smoke_command
    assert "--live-completion-timeout 240" in smoke_command
    assert "--live-stop-timeout 45" in smoke_command
    windows_smoke_step = next(
        step
        for step in smoke_platform["steps"]
        if step.get("name") == "Smoke Windows bundled runtime install and CLI"
    )
    assert windows_smoke_step["if"] == (
        "matrix.platform_key == 'windows-x86_64'"
    )
    windows_smoke_command = _step_run(
        smoke_platform,
        "Smoke Windows bundled runtime install and CLI",
    )
    assert "--require-bundled-current-platform" in windows_smoke_command
    assert "--check-executable-version" in windows_smoke_command
    assert "--start-runtime" not in windows_smoke_command
    assert "--live-startup-timeout 45" in windows_smoke_command
    assert "--live-stop-timeout 45" in windows_smoke_command
    assert "--hermetic-runtime-smoke" not in windows_smoke_command
    assert "--default-routing-runtime-smoke" not in windows_smoke_command

    assemble = jobs["assemble-release-artifacts"]
    assert isinstance(assemble, dict)
    assert assemble["needs"] == "build-metadata"
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


def test_release_workflow_publishes_metadata_only_dist_artifact() -> None:
    """Trusted Publishing artifact scope should match normal PyPI policy."""
    workflow = _release_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)

    build_metadata = jobs["build-metadata"]
    assert isinstance(build_metadata, dict)
    assert build_metadata["needs"] == "validate-release-inputs"

    assemble = jobs["assemble-release-artifacts"]
    assert isinstance(assemble, dict)
    assert assemble["needs"] == "build-metadata"
    step_names = [
        step.get("name") for step in assemble["steps"] if isinstance(step, dict)
    ]
    assert "Download metadata distributions" in step_names
    assert "Download bundled platform wheels" not in step_names

    reports_step = _step(assemble, "Download release reports")
    assert reports_step["with"] == {
        "name": "histdatacom-metadata-reports",
        "path": "release-reports",
    }
    verify_command = _step_run(
        assemble, "Verify assembled release distributions"
    )
    assert "expected 1 metadata wheel" in verify_command
    assert "expected 6 wheels" not in verify_command
    assert "expected 1 sdist" in verify_command

    upload_step = _step(assemble, "Upload release distributions")
    assert upload_step["with"] == {
        "name": "histdatacom-dist",
        "path": "dist/*",
        "if-no-files-found": "error",
    }

    assert jobs["publish-testpypi"]["needs"] == "assemble-release-artifacts"
    assert jobs["publish-pypi"]["needs"] == "assemble-release-artifacts"


def test_package_metadata_advertises_platform_wheel_support() -> None:
    """PyPI metadata should match the runtime platform wheel support matrix."""
    project = _pyproject_config()["project"]
    assert isinstance(project, dict)
    classifiers = set(project["classifiers"])

    assert {
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
    } <= classifiers


def test_package_metadata_advertises_optional_models_provider() -> None:
    """Rich fitted models should remain isolated in the models/all extras."""
    optional = _pyproject_config()["project"]["optional-dependencies"]

    assert optional["models"] == [
        "arch>=8.0.0,<9",
        "statsmodels>=0.14.6,<0.15",
    ]
    assert "arch>=8.0.0,<9" in optional["all"]
    assert "statsmodels>=0.14.6,<0.15" in optional["all"]
    assert "arch==8.0.0" in optional["test"]
    assert "statsmodels==0.14.6" in optional["test"]
    assert "arch==8.0.0" in optional["dev"]
    assert "statsmodels==0.14.6" in optional["dev"]


def test_package_metadata_advertises_optional_duckdb_query_provider() -> None:
    """DuckDB stays optional at runtime and pinned in verification extras."""
    optional = _pyproject_config()["project"]["optional-dependencies"]

    assert optional["query"] == ["duckdb>=1.5.4,<2"]
    assert "duckdb>=1.5.4,<2" in optional["all"]
    assert "duckdb==1.5.4" in optional["test"]
    assert "duckdb==1.5.4" in optional["dev"]


def test_runtime_runbook_documents_windows_runtime_support_gap() -> None:
    """Release docs should state the current Windows runtime support boundary."""
    runbook = _project_text("docs/temporal-orchestration-runtime-runbook.md")
    readme = _project_text("README.md")

    assert "Windows bundled wheels are currently install/CLI-only" in runbook
    assert "windows-2022" in runbook
    assert "temporalio==1.28.0" in runbook
    assert "nexus-rpc==1.4.0" in runbook
    assert "0xC0000142" in runbook
    assert "install/CLI-only" in readme


def test_local_publishing_script_enforces_branch_contract() -> None:
    """Local release uploads should map dev to TestPyPI and main to PyPI."""
    script = _project_text("pypi.sh")

    assert 'testpypi_branch="${HISTDATACOM_TESTPYPI_BRANCH:-dev}"' in script
    assert 'pypi_branch="${HISTDATACOM_PYPI_BRANCH:-main}"' in script
    assert 'prepare_release_upload "TestPyPI" "${testpypi_branch}"' in script
    assert 'prepare_release_upload "PyPI" "${pypi_branch}"' in script
    assert "HISTDATACOM_ALLOW_RELEASE_BRANCH_MISMATCH" in script
    assert "refusing release upload with uncommitted tracked changes" in script
    assert 'fetch_report="${HISTDATACOM_FETCH_REPORT:-}"' in script
    assert "Set HISTDATACOM_FETCH_REPORT" in script
    assert '--fetch-report "${fetch_report}"' in script
    assert "--check-version" in script
    assert "python -m twine check dist/*.whl dist/*.tar.gz" in script
    assert "HISTDATACOM_SKIP_GPG_SIGNING" in script
    assert "HISTDATACOM_GPG_KEY" in script
    assert "require_release_signing_ready" in script
    assert "clear_dist_signatures" in script
    assert "--local-user" in script
    assert "gpg_args=(--batch --yes)" in script
    assert "HISTDATACOM_MAX_UPLOAD_FILE_BYTES" in script
    assert "HISTDATACOM_ALLOW_OVERSIZE_UPLOAD" in script
    assert "validate_dist_artifact_sizes" in script
    assert "python -m twine check dist/*.whl dist/*.tar.gz" in script
    assert (
        "python -m twine check dist/*.whl dist/*.tar.gz\n"
        "    validate_dist_artifact_sizes"
    ) in script
    assert "upload_dist_artifacts pypi" in script
    assert "upload_dist_artifacts testpypi" in script
    assert "testpypi_preflight)" in script
    assert "testpypi_preflight()" in script
    assert "scripts/build_local_simple_index.py" in script
    assert '"file://${local_index}/simple/"' in script
    assert "dist/testpypi-preflight-report.json" in script
    assert "verify_release_install" in script
    assert "scripts/verify_testpypi_install.py" in script
    assert '--version "$(current_package_version)"' in script
    assert "--require-external-runtime-provisioning" in script
    assert "--live-stop-timeout 90" in script
    assert "--download-smoke" in script
    assert 'python -m twine upload -r "${repository}"' in script


def test_local_pypi_install_smoke_uses_exact_version_verifier() -> None:
    """PyPI install smoke should verify the exact release version from PyPI."""
    script = _project_text("pypi.sh")

    assert "pypi_install()" in script
    assert (
        'echo "${bold}verifying histdatacom from pypi: '
        'https://pypi.org/simple/${normal}"'
    ) in script
    assert (
        "verify_release_install \\\n"
        '        "https://pypi.org/simple/" \\\n'
        '        "dist/pypi-install-report.json"'
    ) in script
    assert "python -m pip install histdatacom" not in script
    assert "histdatacom_test()" not in script
    assert "buildenv()" not in script
    assert "destroyenv()" not in script
    assert "pypi_install)\n            pypi_install\n            ;;" in script


def test_release_docs_mark_local_publishing_as_current_path() -> None:
    """Release docs should not imply Actions deployment is active today."""
    release_docs = _project_text("RELEASE.md")
    readme = _project_text("README.md")

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
    assert "bash pypi.sh testpypi_preflight" in release_docs
    assert "dist/testpypi-preflight-report.json" in release_docs
    assert "dist/pypi-install-report.json" in release_docs
    assert "`histdatacom==$(current_package_version)`" in release_docs
    assert "pip's cache disabled" in release_docs
    assert "dist/local-simple-index-report.json" in release_docs
    assert "`bash pypi.sh pypi` is guarded to run from `main`" in release_docs
    assert "HISTDATACOM_FETCH_REPORT" in release_docs
    assert "HISTDATACOM_SKIP_GPG_SIGNING=1" in release_docs
    assert "HISTDATACOM_ALLOW_OVERSIZE_UPLOAD=1" in release_docs
    assert "HISTDATACOM_MAX_UPLOAD_FILE_BYTES" in release_docs
    assert "keyring" in release_docs
    assert "HISTDATACOM_GPG_KEY" in release_docs
    assert "non-interactive GPG" in release_docs
    assert "stale `dist/*.asc`" in release_docs
    assert "HISTDATACOM_TEMPORAL_CACHE_DIR" in release_docs
    assert "network access" in release_docs
    assert "python -m twine check dist/*.whl dist/*.tar.gz" in release_docs
    assert "scripts/fetch_temporal_cli.py" in release_docs
    assert "external Temporal runtime resolver" in release_docs
    assert "metadata-only universal" in readme
    assert "wheel and source distribution" in readme
    assert "include_bundled_platform_wheels=true" in readme
    assert "bundled_platform_wheel_size_confirmed=true" in readme
    assert re.search(
        r"not\s+consumed by TestPyPI/PyPI publish jobs",
        readme,
    )
    assert "histdatacom-dist" in release_docs
    assert (
        "metadata-only universal wheel and source distribution" in release_docs
    )
    assert "include_bundled_platform_wheels=true" in release_docs
    assert "bundled_platform_wheel_size_confirmed=true" in release_docs
    assert re.search(r"not\s+consumed by\s+the\s+publish jobs", release_docs)
    assert "must build all bundled platform wheels" not in release_docs

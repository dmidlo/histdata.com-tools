"""Tests for orchestration documentation boundaries."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read_doc(relative_path: str) -> str:
    """Read a repository documentation file."""
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")


def test_user_orchestration_guide_avoids_runtime_internals() -> None:
    """The primary operations guide should stay focused on user workflows."""
    guide = _read_doc("docs/temporal-orchestration-operations.md")

    assert "# Temporal Orchestration User Guide" in guide
    assert "## Submit CLI Work" in guide
    assert "## Job Telemetry" in guide
    assert "histdatacom jobs progress" in guide
    assert "histdatacom jobs artifacts" in guide
    assert "histdatacom jobs retry" in guide
    assert "histdatacom --quality" in guide
    assert "temporal-orchestration-runtime-runbook.md" in guide

    internal_terms = (
        "## Runtime Paths",
        "## Ports",
        "## Lifecycle Commands",
        "## Maintenance And Retention",
        "## Workers And Task Queues",
        "## Contributor Testing Strategy",
        "state/runtime.pid.json",
        "sqlite/temporal.db",
        "temporal-worker-<lane>.log",
        "sidecar",
        "migration-era",
    )
    for term in internal_terms:
        assert term not in guide


def test_runtime_runbook_retains_maintainer_details() -> None:
    """The maintainer runbook should retain low-level runtime guidance."""
    runbook = _read_doc("docs/temporal-orchestration-runtime-runbook.md")

    assert "# Temporal Orchestration Runtime Runbook" in runbook
    assert "## Runtime Paths" in runbook
    assert "## Ports" in runbook
    assert "## Lifecycle Commands" in runbook
    assert "## Maintenance And Retention" in runbook
    assert "## Workers And Task Queues" in runbook
    assert "state/runtime.pid.json" in runbook
    assert "sqlite/temporal.db" in runbook
    assert "temporal-worker-<lane>.log" in runbook
    assert "temporal-orchestration-operations.md" in runbook


def test_readme_links_user_and_maintainer_orchestration_docs() -> None:
    """README should route users and maintainers to different docs."""
    readme = _read_doc("README.md")

    assert "Temporal Orchestration User Guide" in readme
    assert "docs/temporal-orchestration-operations.md" in readme
    assert "Temporal Orchestration Runtime Runbook" in readme
    assert "docs/temporal-orchestration-runtime-runbook.md" in readme
    assert "for submit, observe, cancel, retry, resume" in readme
    assert "for maintainer lifecycle commands" in readme

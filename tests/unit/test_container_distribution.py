"""Contract tests for the application container distribution surface."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def _workflow() -> dict[str, object]:
    loaded = yaml.safe_load(_text(".github/workflows/container.yml"))
    assert isinstance(loaded, dict)
    return loaded


def _triggers(workflow: dict[str, object]) -> dict[str, object]:
    triggers = workflow.get("on", workflow.get(True))
    assert isinstance(triggers, dict)
    return triggers


def test_docker_build_context_is_a_strict_package_allow_list() -> None:
    """Large repository data and local state must never enter the context."""
    assert _text(".dockerignore").splitlines() == [
        "**",
        "!Dockerfile",
        "!pyproject.toml",
        "!README.md",
        "!LICENSE",
        "!container",
        "!container/constraints.txt",
        "!src",
        "!src/**",
    ]
    assert "!container/constraints.txt" in _text(".gitignore").splitlines()


def test_dockerfile_preserves_rootless_one_shot_runtime_contract() -> None:
    """The image should be pinned, rootless, writable, and CLI-oriented."""
    dockerfile = _text("Dockerfile")
    assert (
        "python:3.13-slim-bookworm@sha256:"
        "9d7f287598e1a5a978c015ee176d8216435aaf335ed69ac3c38dd1bbb10e8d64"
        in dockerfile
    )
    assert dockerfile.count("FROM ${PYTHON_BASE}") == 2
    assert "--constraint container/constraints.txt" in dockerfile
    assert "--no-build-isolation" in dockerfile
    assert "--wheel-dir /wheels" in dockerfile
    assert "--no-index" in dockerfile
    assert "tini=0.19.0-1+b3" in dockerfile
    assert "WORKDIR /workspace" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert 'ENTRYPOINT ["/usr/bin/tini", "--", "histdatacom"]' in dockerfile
    assert 'CMD ["--help"]' in dockerfile
    for name, path in {
        "HISTDATACOM_RUNTIME_HOME": "/workspace/runtime",
        "HISTDATACOM_RUNTIME_WORKSPACE": "/workspace",
        "HISTDATACOM_TEMPORAL_CACHE_DIR": ("/workspace/cache/temporal-cli"),
    }.items():
        assert f"{name}={path}" in dockerfile
    assert not re.search(
        r"^\s*(HEALTHCHECK|VOLUME)\b", dockerfile, re.MULTILINE
    )
    assert dockerfile.count("org.opencontainers.image.") >= 8


def test_container_dependency_graph_is_exactly_constrained() -> None:
    """Container rebuilds should not resolve mutable Python version ranges."""
    constraints = [
        line
        for line in _text("container/constraints.txt").splitlines()
        if line and not line.startswith("#")
    ]
    assert len(constraints) >= 20
    assert all(
        re.fullmatch(r"[A-Za-z0-9_.-]+==[^=\s]+", line) for line in constraints
    )
    for required in (
        "polars==1.42.1",
        "packaging==26.2",
        "temporalio==1.30.0",
        "setuptools==83.0.0",
        "wheel==0.47.0",
    ):
        assert required in constraints


def test_container_workflow_avoids_dev_pushes_and_publishes_tags_only() -> None:
    """Image credits and package writes should be confined to their policy."""
    workflow = _workflow()
    triggers = _triggers(workflow)
    push = triggers["push"]
    assert isinstance(push, dict)
    assert push == {"tags": ["v*"]}
    pull_request = triggers["pull_request"]
    assert isinstance(pull_request, dict)
    paths = pull_request["paths"]
    assert isinstance(paths, list)
    assert "Dockerfile" in paths
    assert ".dockerignore" in paths
    assert "container/constraints.txt" in paths
    assert "src/**" not in paths
    assert "pyproject.toml" not in paths

    assert workflow["permissions"] == {"contents": "read"}
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    validate = jobs["validate"]
    publish = jobs["publish"]
    assert isinstance(validate, dict)
    assert isinstance(publish, dict)
    assert "permissions" not in validate
    assert publish["if"] == (
        "github.event_name == 'push' && "
        "startsWith(github.ref, 'refs/tags/v')"
    )
    assert publish["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "packages": "write",
    }

    workflow_text = _text(".github/workflows/container.yml")
    assert "coverage" not in workflow_text.lower()
    assert "linux/amd64,linux/arm64" in workflow_text
    assert "provenance: mode=max" in workflow_text
    assert "sbom: true" in workflow_text
    assert "docker/setup-qemu-action@v4.2.0" in workflow_text
    assert "docker/setup-buildx-action@v4.2.0" in workflow_text
    assert "docker/login-action@v4.4.0" in workflow_text
    assert "docker/metadata-action@v6.2.0" in workflow_text
    assert "docker/build-push-action@v7.3.0" in workflow_text


def test_container_documentation_covers_storage_and_process_lifecycle() -> None:
    """Operators should see the persistence boundary and one-shot semantics."""
    guide = _text("docs/container.md")
    readme = _text("README.md")
    index = _text("docs/index.rst")
    for phrase in (
        "one-shot command-line tool",
        "histdatacom-workspace",
        "10001:10001",
        "checksum-verified Temporal binary",
        "--deep-runtime",
        "ordinary `dev` pushes",
    ):
        assert phrase in guide
    assert "[container guide](docs/container.md)" in readme
    assert "   container" in index

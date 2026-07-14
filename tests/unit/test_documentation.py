"""Repository contracts for the maintained documentation build."""

import re
from importlib import metadata
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_ROOT = REPO_ROOT / "docs"
TOCTREE_ENTRY = re.compile(r"^   ([a-z0-9][a-z0-9_./-]*)$", re.MULTILINE)


def test_read_the_docs_uses_the_versioned_sphinx_configuration() -> None:
    config = yaml.safe_load(
        (REPO_ROOT / ".readthedocs.yaml").read_text(encoding="utf-8")
    )

    assert config["version"] == 2
    assert config["sphinx"] == {
        "configuration": "docs/conf.py",
        "fail_on_warning": True,
    }
    assert config["python"]["install"] == [
        {
            "method": "pip",
            "path": ".",
            "extra_requirements": ["docs"],
        }
    ]


def test_documentation_extra_is_python_310_compatible_and_pinned() -> None:
    requirements = metadata.requires("histdatacom") or []
    docs_requirements = sorted(
        requirement.partition(";")[0].strip()
        for requirement in requirements
        if 'extra == "docs"' in requirement
    )

    assert docs_requirements == sorted(
        [
            "myst-parser==4.0.1",
            "sphinx==8.1.3",
            "sphinx-rtd-theme==3.1.0",
        ]
    )


def test_ci_builds_documentation_without_coverage() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
    )
    docs_job = workflow["jobs"]["docs"]
    commands = "\n".join(step.get("run", "") for step in docs_job["steps"])

    assert (
        "python -m sphinx -W --keep-going -b html docs docs/_build/html"
        in commands
    )
    assert "--cov" not in commands


def test_every_maintained_markdown_document_is_in_the_root_toctree() -> None:
    index = (DOCS_ROOT / "index.rst").read_text(encoding="utf-8")
    included_documents = set(TOCTREE_ENTRY.findall(index))
    maintained_documents = {
        path.relative_to(DOCS_ROOT).with_suffix("").as_posix()
        for path in DOCS_ROOT.rglob("*.md")
    }

    assert included_documents == maintained_documents

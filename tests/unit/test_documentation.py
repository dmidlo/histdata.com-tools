"""Repository contracts for the maintained documentation build."""

import re
from importlib import metadata
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_ROOT = REPO_ROOT / "docs"
TOCTREE_ENTRY = re.compile(r"^   ([A-Za-z0-9_./-]+)$", re.MULTILINE)


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
    """Keep Markdown and reStructuredText guides in the maintained root tree."""
    _assert_root_toctree(DOCS_ROOT)


def _assert_root_toctree(docs_root: Path) -> None:
    index = (docs_root / "index.rst").read_text(encoding="utf-8")
    included_documents = set(TOCTREE_ENTRY.findall(index))
    maintained_names = [
        path.relative_to(docs_root).with_suffix("").as_posix()
        for path in docs_root.rglob("*")
        if path.is_file()
        and path.suffix in {".md", ".rst"}
        and path != docs_root / "index.rst"
        and path.relative_to(docs_root).parts[0] != "_build"
    ]
    maintained_documents = set(maintained_names)

    assert len(maintained_documents) == len(maintained_names)
    assert included_documents == maintained_documents


def test_documentation_tree_accepts_both_formats_and_ignores_builds(
    tmp_path: Path,
) -> None:
    """The root and generated pages are not maintained standalone guides."""
    for name in (
        "guide.md",
        "reference.rst",
        "nested/index.rst",
        "_build/html/generated.md",
        "_build/html/generated.rst",
        "asset.txt",
    ):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("Fixture\n=======\n", encoding="utf-8")
    (tmp_path / "index.rst").write_text(
        ".. toctree::\n\n   guide\n   reference\n   nested/index\n",
        encoding="utf-8",
    )

    _assert_root_toctree(tmp_path)


def test_documentation_tree_rejects_ambiguous_mixed_format_stem(
    tmp_path: Path,
) -> None:
    """One toctree name cannot select two different maintained source files."""
    for suffix in (".md", ".rst"):
        (tmp_path / ("guide" + suffix)).write_text("Fixture", encoding="utf-8")
    (tmp_path / "index.rst").write_text(
        ".. toctree::\n\n   guide\n", encoding="utf-8"
    )

    with pytest.raises(AssertionError):
        _assert_root_toctree(tmp_path)


@pytest.mark.parametrize("suffix", (".md", ".rst"))
def test_documentation_tree_rejects_unlisted_maintained_guide(
    tmp_path: Path, suffix: str
) -> None:
    """Adding either supported format requires adding its toctree entry."""
    (tmp_path / ("missing" + suffix)).write_text("Fixture", encoding="utf-8")
    (tmp_path / "index.rst").write_text(".. toctree::\n", encoding="utf-8")

    with pytest.raises(AssertionError):
        _assert_root_toctree(tmp_path)


@pytest.mark.parametrize(
    "entry", ("nonexistent", "_build/generated", ".hidden", "index")
)
def test_documentation_tree_rejects_surplus_entries(
    tmp_path: Path, entry: str
) -> None:
    """Missing, generated, and root-only pages cannot mask an orphan entry."""
    (tmp_path / "_build").mkdir()
    (tmp_path / "_build/generated.rst").write_text("Fixture", encoding="utf-8")
    (tmp_path / "index.rst").write_text(
        f".. toctree::\n\n   {entry}\n", encoding="utf-8"
    )

    with pytest.raises(AssertionError):
        _assert_root_toctree(tmp_path)

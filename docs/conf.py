"""Sphinx configuration for the HistData.com Tools documentation."""

from importlib import metadata

project = "HistData.com Tools"
author = "David Midlo"
copyright = "2026, David Midlo"

release = metadata.version("histdatacom")
version = ".".join(release.split(".")[:2])

extensions = ["myst_parser"]
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
root_doc = "index"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

myst_heading_anchors = 4
myst_enable_extensions = ["colon_fence", "deflist", "fieldlist"]

html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 4,
}
html_context = {
    "display_github": True,
    "github_user": "dmidlo",
    "github_repo": "histdata.com-tools",
    "github_version": "dev",
    "conf_py_path": "/docs/",
}

"""Ordinary setuptools builds refuse stale public metadata; no host imports."""

import runpy
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist

ROOT = Path(__file__).parent


def check_metadata():
    functions = runpy.run_path(str(ROOT / "tools" / "generate_metadata.py"))
    functions["check_metadata"](ROOT)


class CheckedBuild(build_py):
    def run(self):
        check_metadata()
        super().run()


class CheckedSdist(sdist):
    def run(self):
        check_metadata()
        super().run()


setup(cmdclass={"build_py": CheckedBuild, "sdist": CheckedSdist})

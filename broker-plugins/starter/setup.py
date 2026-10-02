"""Build without importing the plugin/host; reject stale generated resources."""

import hashlib
import json
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist

ROOT = Path(__file__).resolve().parent


def check_metadata() -> None:
    lock = ROOT / "metadata-lock.json"
    if not lock.is_file():
        raise RuntimeError(
            "Run python tools/generate_metadata.py with the reviewed host installed"
        )
    hashes = json.loads(lock.read_text("ascii"))
    required = {
        "pyproject.toml",
        "setup.py",
        "README.md",
        "docs/CONFORMANCE_ABI.md",
        "tools/generate_metadata.py",
        "tools/check_host.py",
        "src/histdatacom_broker_starter/__init__.py",
        "src/histdatacom_broker_starter/plugin.py",
        "src/histdatacom_broker_starter/_histdatacom_broker_plugins/org.example.starter.json",
        "src/histdatacom_broker_starter/_histdatacom_broker_permissions/org.example.starter.json",
        "conformance/driver.json",
    }
    if set(hashes) != required:
        raise RuntimeError("Metadata lock inventory differs; regenerate declarations")
    for relative, expected in hashes.items():
        path = ROOT / relative
        if (
            not path.is_file()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ):
            raise RuntimeError("Stale metadata: run python tools/generate_metadata.py")


class CheckedBuild(build_py):
    def run(self) -> None:
        check_metadata()
        super().run()


class CheckedSdist(sdist):
    def run(self) -> None:
        check_metadata()
        super().run()


setup(cmdclass={"build_py": CheckedBuild, "sdist": CheckedSdist})

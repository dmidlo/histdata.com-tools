"""Test --version argument from API and CLI."""

import subprocess
import sys

import histdatacom
from histdatacom import Options


def test_api_version() -> None:
    """Test --version from API."""
    options = Options()
    options.version = True
    version = histdatacom(options)  # type: ignore # pylint: disable=not-callable

    assert version == histdatacom.__version__  # act


def test_cli_version() -> None:
    """Test --version from CLI."""
    cli_output = subprocess.run(
        [sys.executable, "-m", "histdatacom", "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    assert cli_output.strip() == histdatacom.__version__  # act

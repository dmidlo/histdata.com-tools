"""Pytest unit tests for histdatacom.cli.py."""

import sys

import pytest

from histdatacom import Options
from histdatacom.cli import ArgParser


def test_cli() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_unsupported_format_timeframe_combination_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject CLI requests that would generate zero supported URLs."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "metatrader",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    with pytest.raises(SystemExit) as err:
        ArgParser(Options())()

    assert err.value.code == 1


@pytest.mark.parametrize(
    "argv",
    (
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-13",
        ],
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
            "-c",
            "banana",
        ],
    ),
)
def test_invalid_cli_inputs_exit_nonzero(
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validation failures should fail for shell automation."""
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as err:
        ArgParser(Options())()

    assert err.value.code == 1

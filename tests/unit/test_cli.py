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


def test_sidecar_cli_flags_preserve_default_runtime_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sidecar flags should preserve normal CLI validation."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--sidecar",
            "--sidecar-start",
            "--sidecar-submit-only",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert options.use_sidecar
    assert options.sidecar_start
    assert not options.sidecar_wait_result
    assert options.validate_urls


def test_foreground_cli_flag_opts_out_of_default_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators should have an explicit foreground rollback switch."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--foreground",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert not options.use_sidecar
    assert options.sidecar_start
    assert options.validate_urls


def test_no_sidecar_start_cli_flag_requires_running_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators should be able to disable default sidecar autostart."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--no-sidecar-start",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert options.use_sidecar
    assert not options.sidecar_start
    assert options.validate_urls


@pytest.mark.parametrize(
    ("flag", "expected"),
    (
        (
            "-A",
            {
                "available_remote_data": True,
                "update_remote_data": False,
                "validate_urls": False,
                "download_data_archives": False,
                "extract_csvs": False,
                "import_to_influxdb": False,
            },
        ),
        (
            "-U",
            {
                "available_remote_data": False,
                "update_remote_data": True,
                "validate_urls": False,
                "download_data_archives": False,
                "extract_csvs": False,
                "import_to_influxdb": False,
            },
        ),
        (
            "-V",
            {
                "available_remote_data": False,
                "update_remote_data": False,
                "validate_urls": True,
                "download_data_archives": False,
                "extract_csvs": False,
                "import_to_influxdb": False,
            },
        ),
        (
            "-D",
            {
                "available_remote_data": False,
                "update_remote_data": False,
                "validate_urls": True,
                "download_data_archives": True,
                "extract_csvs": False,
                "import_to_influxdb": False,
            },
        ),
        (
            "-X",
            {
                "available_remote_data": False,
                "update_remote_data": False,
                "validate_urls": True,
                "download_data_archives": True,
                "extract_csvs": True,
                "import_to_influxdb": False,
            },
        ),
        (
            "-I",
            {
                "available_remote_data": False,
                "update_remote_data": False,
                "validate_urls": True,
                "download_data_archives": True,
                "extract_csvs": False,
                "import_to_influxdb": True,
            },
        ),
    ),
)
def test_legacy_behavior_flags_keep_sidecar_request_shape(
    flag: str,
    expected: dict[str, bool],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy CLI flags should parse the same before sidecar submission."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--sidecar",
            flag,
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    for name, value in expected.items():
        assert getattr(options, name) is value

"""Pytest unit tests for histdatacom.cli.py."""

import json
from pathlib import Path
import sys

import pytest

from histdatacom import Options
from histdatacom.data_quality import QUALITY_PROFILE_SCHEMA_VERSION
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


def test_orchestration_cli_flags_preserve_default_runtime_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Orchestration flags should preserve normal CLI validation."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--orchestration-start",
            "--submit-only",
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

    assert options.use_orchestration
    assert options.orchestration_start
    assert not options.orchestration_wait_result
    assert options.validate_urls


def test_orchestration_cli_flags_are_no_longer_public(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The top-level CLI should not accept old orchestration flag spelling."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--orchestration-submit-only",
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

    with pytest.raises(SystemExit) as err:
        ArgParser(Options())()

    assert err.value.code == 2


def test_help_advertises_orchestration_jobs_not_orchestration() -> None:
    """Main help should point users at orchestration job telemetry."""
    parser = ArgParser(Options())
    parser._set_args()
    help_text = parser.format_help()

    assert "Orchestration:" in help_text
    assert "histdatacom jobs --help" in help_text
    assert "--submit-only" in help_text
    assert "--orchestration-submit-only" not in help_text


def test_help_describes_end_yearmonth_as_end_period() -> None:
    """Issue #29: -e help must not describe the start period."""
    parser = ArgParser(Options())
    parser._set_args()
    help_text = parser.format_help()

    assert "set an end year and month for data. e.g. -e 2020-00" in help_text
    assert "set a start year and month for data. e.g. -e" not in help_text


def test_foreground_cli_flag_is_no_longer_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retired foreground rollback switch should fail at parse time."""
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

    with pytest.raises(SystemExit) as err:
        ArgParser(Options())()

    assert err.value.code == 2


def test_no_orchestration_start_cli_flag_requires_running_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators should be able to disable default runtime autostart."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--no-orchestration-start",
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

    assert options.use_orchestration
    assert not options.orchestration_start
    assert options.validate_urls


def test_options_expose_orchestration_named_runtime_controls() -> None:
    """Options should expose public orchestration runtime controls."""
    options = Options()

    options.orchestration_start = False
    options.orchestration_wait_result = False

    assert options.use_orchestration
    assert not options.orchestration_start
    assert not options.orchestration_wait_result


def test_data_quality_cli_mode_bypasses_legacy_prerequisites(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Offline quality mode should not auto-enable network/download stages."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--quality-target",
            str(tmp_path),
            "--quality-checks",
            "inventory",
            "ingestion",
            "--quality-report",
            str(tmp_path / "quality.json"),
            "--quality-fail-on",
            "warning",
            "--quality-max-errors",
            "2",
            "--quality-max-warnings",
            "5",
        ],
    )

    options = ArgParser(Options())()

    assert options.data_quality
    assert options.quality_paths == [str(tmp_path)]
    assert options.quality_check_groups == ["inventory", "ingestion"]
    assert options.quality_report_path == str(tmp_path / "quality.json")
    assert options.quality_fail_on == "warning"
    assert options.quality_max_errors == 2
    assert options.quality_max_warnings == 5
    assert not options.validate_urls
    assert not options.download_data_archives
    assert not options.extract_csvs
    assert not options.import_to_influxdb


def test_data_quality_cli_loads_quality_profile_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Quality mode should embed validated JSON profile files."""
    profile_path = tmp_path / "quality-profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "cli-profile",
                "rules": {"ingestion.ascii.row_count": {"min_row_count": 10}},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--quality-target",
            str(tmp_path),
            "--quality-profile",
            str(profile_path),
        ],
    )

    options = ArgParser(Options())()

    assert options.quality_profile_path == str(profile_path)
    assert options.quality_profile["name"] == "cli-profile"
    assert options.quality_profile["source"] == "file"
    assert options.quality_profile["source_path"] == str(profile_path)


def test_data_quality_cli_defaults_to_data_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Operators can run quality mode against the configured data directory."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--data-directory",
            str(tmp_path),
        ],
    )

    options = ArgParser(Options())()

    assert options.data_quality
    assert options.quality_paths == (str(tmp_path),)
    assert not options.validate_urls


def test_repo_quality_cli_refresh_defaults_to_data_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Repo-quality refresh should be explicit and local-only."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--repo-quality",
            "--quality-checks",
            "inventory",
            "--data-directory",
            str(tmp_path),
        ],
    )

    options = ArgParser(Options())()

    assert not options.data_quality
    assert options.repo_quality_refresh
    assert options.quality_paths == (str(tmp_path),)
    assert options.quality_check_groups == ["inventory"]
    assert not options.available_remote_data
    assert not options.update_remote_data
    assert not options.validate_urls


def test_repo_quality_columns_are_display_only_for_repo_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stored quality columns should not imply a quality refresh."""
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "-A", "--repo-quality-columns"],
    )

    options = ArgParser(Options())()

    assert options.available_remote_data
    assert options.repo_quality_columns
    assert not options.repo_quality_refresh
    assert not options.data_quality


@pytest.mark.parametrize(
    "argv",
    (
        ["histdatacom", "--quality-target", "data"],
        ["histdatacom", "--quality-checks", "inventory"],
        ["histdatacom", "--quality-report", "quality.json"],
        ["histdatacom", "--quality-fail-on", "warning"],
        ["histdatacom", "--quality-max-errors", "1"],
        ["histdatacom", "--quality-max-warnings", "1"],
        ["histdatacom", "--quality-profile", "quality-profile.json"],
        ["histdatacom", "--quality", "-D"],
        ["histdatacom", "--repo-quality", "-A"],
        ["histdatacom", "--repo-quality-columns"],
    ),
)
def test_data_quality_cli_rejects_ambiguous_quality_flags(
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Quality mode should stay separate from legacy network operations."""
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as err:
        ArgParser(Options())()

    assert err.value.code == 1


def test_api_options_ignore_ambient_process_argv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API parsing should not depend on the executable path or pytest flags."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "/tmp/histdatacom-py310-ci/bin/python",
            "--cov=histdatacom",
            "--cov-report=xml",
        ],
    )
    options = Options()
    options.from_api = True
    options.version = True

    parsed = ArgParser(options)()

    assert parsed.version is True
    assert parsed.from_api is True


def test_api_repo_quality_refresh_accepts_default_format_matrix(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Repo quality API requests should not validate legacy fetch formats."""
    monkeypatch.setattr(sys, "argv", ["histdatacom"])
    options = Options()
    options.from_api = True
    options.repo_quality_refresh = True
    options.data_directory = str(tmp_path)

    parsed = ArgParser(options)()

    assert parsed.repo_quality_refresh is True
    assert parsed.quality_paths == (str(tmp_path),)


def test_api_quality_options_accept_inline_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """API callers can pass an inline quality profile dict."""
    monkeypatch.setattr(sys, "argv", ["histdatacom"])
    options = Options()
    options.from_api = True
    options.data_quality = True
    options.quality_paths = (str(tmp_path),)
    options.quality_profile = {
        "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
        "name": "api-profile",
        "modeling_assumptions": {"target_horizon_minutes": 5},
    }

    parsed = ArgParser(options)()

    assert parsed.data_quality is True
    assert parsed.quality_profile["name"] == "api-profile"
    assert parsed.quality_profile["source"] == "api-options"
    assert parsed.quality_profile["modeling_assumptions"] == {
        "target_horizon_minutes": 5
    }


def test_argparser_bare_construction_uses_fresh_option_namespace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Repeated default parser construction should not share CLI state."""
    first_data_dir = tmp_path / "first"
    second_data_dir = tmp_path / "second"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--no-orchestration-start",
            "--submit-only",
            "-I",
            "-d",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "1-minute-bar-quotes",
            "-s",
            "2022-12",
            "--data-directory",
            str(first_data_dir),
        ],
    )

    first_options = ArgParser()()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "gbpusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2021-11",
            "--data-directory",
            str(second_data_dir),
        ],
    )

    second_options = ArgParser()()

    assert first_options.pairs == ["eurusd"]
    assert first_options.formats == ["ascii"]
    assert first_options.timeframes == ["M1"]
    assert first_options.data_directory == str(first_data_dir)
    assert first_options.validate_urls
    assert first_options.download_data_archives
    assert first_options.extract_csvs
    assert first_options.import_to_influxdb
    assert first_options.delete_after_influx
    assert not first_options.orchestration_start
    assert not first_options.orchestration_wait_result

    assert second_options.pairs == ["gbpusd"]
    assert second_options.formats == ["ascii"]
    assert second_options.timeframes == ["T"]
    assert second_options.data_directory == str(second_data_dir)
    assert second_options.validate_urls
    assert not second_options.download_data_archives
    assert not second_options.extract_csvs
    assert not second_options.import_to_influxdb
    assert not second_options.delete_after_influx
    assert second_options.orchestration_start
    assert second_options.orchestration_wait_result


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
                "extract_csvs": True,
                "import_to_influxdb": True,
            },
        ),
    ),
)
def test_behavior_flags_keep_orchestration_request_shape(
    flag: str,
    expected: dict[str, bool],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Behavior CLI flags should parse the same before orchestration."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
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

"""Pytest unit tests for histdatacom.cli.py."""

import json
from pathlib import Path
import sys

import pytest

from histdatacom import Options
from histdatacom.data_quality import QUALITY_PROFILE_SCHEMA_VERSION
from histdatacom.cli import ArgParser
from histdatacom.fx_enums import (
    MAJOR_TRIANGLE_PAIR_GROUPS,
    MAJOR_TRIANGLE_SYMBOLS,
)


def test_cli() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_unsupported_format_timeframe_combination_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject retired formats at argument parsing."""
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

    assert err.value.code == 2


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
            "--keep-runtime",
            "--submit-only",
            "--no-overlap",
            "--schedule-key",
            "eurusd-cache",
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
    assert options.orchestration_keep_runtime
    assert not options.orchestration_wait_result
    assert options.no_overlap
    assert options.schedule_key == "eurusd-cache"
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
    assert "analytics   Run offline data analytics operations" in help_text
    assert "groups      List instrument groups and major triangles" in help_text
    assert (
        "datasets    Resolve and verify versioned local datasets" in help_text
    )
    assert "quality     Inspect local data quality evidence" in help_text
    assert "histdatacom analytics --help" in help_text
    assert "histdatacom groups --help" in help_text
    assert "histdatacom datasets --help" in help_text
    assert "histdatacom jobs --help" in help_text
    assert "histdatacom quality --help" in help_text
    assert "--config PATH" in help_text
    assert "--submit-only" in help_text
    assert "--orchestration-submit-only" not in help_text


def test_help_advertises_verbosity_flags() -> None:
    """Issue #13: main help should expose -v/-vv/-vvv."""
    parser = ArgParser(Options())
    parser._set_args()
    help_text = parser.format_help()

    assert "-v, --verbose" in help_text
    assert "-vvv for trace" in help_text


def test_help_advertises_build_cache_mode() -> None:
    """Cache-only mode should be visible from the main command help."""
    parser = ArgParser(Options())
    parser._set_args()
    help_text = parser.format_help()

    assert "-C, --build-cache" in help_text
    assert "--cache-only" in help_text
    assert "Polars .data caches" in help_text


def test_help_advertises_quality_preflight_mode() -> None:
    """Cache-scale quality preflight should be visible from main help."""
    parser = ArgParser(Options())
    parser._set_args()
    help_text = parser.format_help()

    assert "--quality-preflight" in help_text
    assert "--quality-preflight-report" in help_text
    assert "--quality-preflight-evidence" in help_text
    assert "--quality-preflight-evidence-max-age-seconds" in help_text
    assert "--quality-preflight-evidence-stale-ok" in help_text
    assert "--quality-preflight-validation-report" in help_text
    assert "--quality-preflight-validation-evidence" in help_text
    assert "latest" in help_text
    assert "--quality-preflight-run-validation" in help_text
    assert "--quality-preflight-sample-size" in help_text
    assert "--quality-profile-preview" in help_text
    assert "--quality-profile-preview-format" in help_text
    assert "--quality-profile-preview-output" in help_text
    assert "--quality-profile-explain" in help_text
    assert "--quality-remediation-catalog-audit" in help_text


def test_help_advertises_request_json_export() -> None:
    """Main help should expose non-submitting RunRequest export."""
    parser = ArgParser(Options())
    parser._set_args()
    help_text = parser.format_help()

    assert "--request-json-out" in help_text
    assert "--request-bundle-out" in help_text
    assert "without submitting work" in help_text


def test_help_advertises_deterministic_random_windows() -> None:
    """The public help should expose expression, seed, and bounded-session modes."""
    parser = ArgParser(Options())
    parser._set_args()
    help_text = parser.format_help()

    assert "-r, --random-window EXPRESSION" in help_text
    assert "--random-seed INTEGER" in help_text
    assert "all matching occurrences" in " ".join(help_text.split())


def test_help_advertises_pair_groups() -> None:
    """Named instrument groups should be visible from the main help."""
    parser = ArgParser(Options())
    parser._set_args()
    help_text = parser.format_help().replace(
        "major-\n                        triangles",
        "major-triangles",
    )

    assert "--pair-groups" in help_text
    assert "--instrument-groups" in help_text
    assert "majors, minors, crosses, exotics" in help_text
    assert "major-triangles" in help_text


def test_verbose_cli_count_is_normalized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated -v flags should become an integer verbosity level."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-vv",
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

    assert options.verbosity == 2
    assert options.validate_urls


def test_build_cache_cli_implies_download_without_extracting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cache-only mode should plan direct ZIP-to-.data cache builds."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--build-cache",
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

    assert options.build_cache
    assert options.validate_urls
    assert options.download_data_archives
    assert not options.extract_csvs
    assert not options.import_to_influxdb
    assert options.formats == {"ascii"}
    assert options.timeframes == {"T"}


def test_build_cache_cli_filters_default_dimensions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Broad cache requests should not download non-cacheable formats."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--cache-only",
            "-p",
            "eurusd",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert options.build_cache
    assert options.formats == {"ascii"}
    assert options.timeframes == {"T"}


def test_build_cache_cli_rejects_non_cacheable_dimensions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cache-only mode rejects retired formats at argument parsing."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--build-cache",
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

    assert err.value.code == 2


def test_pair_groups_cli_expands_without_defaulting_to_all_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Group-only requests should replace the default all-pair selection."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "--pair-groups",
            "Major",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert set(options.pairs) == {
        "audusd",
        "eurusd",
        "gbpusd",
        "nzdusd",
        "usdcad",
        "usdchf",
        "usdjpy",
    }


def test_pair_groups_cli_accepts_major_triangles_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Major triangle requests should expand to every major triangle symbol."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "--pair-groups",
            "major_triangles",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert tuple(sorted(options.pairs)) == MAJOR_TRIANGLE_SYMBOLS
    assert options.pair_groups == ["major-triangles"]


def test_pair_groups_cli_accepts_unquoted_major_triangles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unquoted major triangles should resolve to the major-triangle basket."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "--pair-groups",
            "major",
            "triangles",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert tuple(sorted(options.pairs)) == MAJOR_TRIANGLE_SYMBOLS
    assert options.pair_groups == ["majors", "major-triangles"]


def test_pair_groups_cli_accepts_individual_triangle_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Individual triangle groups should expand to one relationship."""
    group = "triangle-eurgbp-eurusd-gbpusd"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "--pair-groups",
            "triangle_eurgbp_eurusd_gbpusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert tuple(sorted(options.pairs)) == tuple(
        sorted(MAJOR_TRIANGLE_PAIR_GROUPS[group])
    )
    assert options.pair_groups == [group]


def test_pair_groups_cli_unions_with_explicit_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit symbols should union with named groups."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "--instrument-groups",
            "metals",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
        ],
    )

    options = ArgParser(Options())()

    assert set(options.pairs) == {
        "eurusd",
        "xagusd",
        "xauaud",
        "xauchf",
        "xaueur",
        "xaugbp",
        "xauusd",
    }


def test_config_file_applies_recurrent_run_defaults(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Issue #31: YAML config should cover the top-level command surface."""
    config_path = tmp_path / "histdatacom.yaml"
    data_dir = tmp_path / "data"
    bundle_path = tmp_path / "requests" / "eurusd-cache-bundle.json"
    request_path = tmp_path / "requests" / "eurusd-cache.json"
    config_path.write_text(
        f"""
histdatacom:
  download_data_archives: true
  request_bundle_out: {bundle_path}
  request_json_out: {request_path}
  pairs:
    - eurusd
    - gbpusd
  formats:
    - ascii
  timeframes:
    - tick-data-quotes
  start_yearmonth: 2022-10
  end_yearmonth: 2022-11
  data_directory: {data_dir}
  cpu_utilization: low
  batch_size: 123
  timezone: America/New_York
  keep_runtime: true
  no_overlap: true
  orchestration_start: false
  orchestration_wait_result: false
  schedule_key: eurusd-cache
  verbosity: 2
  analytics:
    command: feed-regimes
    target: data/
  runtime:
    command: status
  jobs:
    command: list
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--config", str(config_path)],
    )

    options = ArgParser(Options())()

    assert options.config_path == str(config_path)
    assert options.pairs == ["eurusd", "gbpusd"]
    assert options.formats == ["ascii"]
    assert options.timeframes == ["T"]
    assert options.start_yearmonth == "202210"
    assert options.end_yearmonth == "202211"
    assert options.data_directory == str(data_dir)
    assert options.cpu_utilization == "low"
    assert options.batch_size == 123
    assert options.output_timezone == "America/New_York"
    assert options.validate_urls
    assert options.download_data_archives
    assert not options.extract_csvs
    assert not options.orchestration_start
    assert options.orchestration_keep_runtime
    assert not options.orchestration_wait_result
    assert options.no_overlap
    assert options.schedule_key == "eurusd-cache"
    assert options.request_bundle_out == str(bundle_path)
    assert options.request_json_out == str(request_path)
    assert options.verbosity == 2


def test_cli_timezone_flag_uses_output_projection_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The short and long public flag should populate output_timezone."""
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "-z", "Europe/London", "--request-json-out", "-"],
    )

    options = ArgParser(Options())()

    assert options.output_timezone == "Europe/London"


def test_api_options_preserve_output_timezone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Programmatic API options should survive the argparse compatibility seam."""
    monkeypatch.setattr(sys, "argv", ["histdatacom"])
    options = Options()
    options.from_api = True
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"T"}
    options.start_yearmonth = "2022-12"
    options.output_timezone = "Asia/Tokyo"

    parsed = ArgParser(options)()

    assert parsed.output_timezone == "Asia/Tokyo"


def test_api_options_preserve_random_window_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Programmatic expression/seed inputs should survive argparse compatibility."""
    monkeypatch.setattr(sys, "argv", ["histdatacom"])
    options = Options()
    options.from_api = True
    options.pairs = {"eurusd"}
    options.formats = {"ascii"}
    options.timeframes = {"T"}
    options.random_window = "90m"
    options.random_seed = 20260714

    parsed = ArgParser(options)()

    assert parsed.random_window == "90m"
    assert parsed.random_seed == 20260714


def test_config_file_keeps_explicit_cli_overrides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Explicit CLI flags should win over recurrent-run defaults."""
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        """
histdatacom:
  validate_urls: true
  pairs: [eurusd]
  formats: [ascii]
  timeframes: [tick-data-quotes]
  start_yearmonth: 2022-10
  verbosity: 3
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--config",
            str(config_path),
            "-p",
            "usdjpy",
            "-t",
            "tick-data-quotes",
            "-s",
            "2022-12",
            "-v",
        ],
    )

    options = ArgParser(Options())()

    assert options.pairs == ["usdjpy"]
    assert options.formats == ["ascii"]
    assert options.timeframes == ["T"]
    assert options.start_yearmonth == "202212"
    assert options.verbosity == 1
    assert options.validate_urls


def test_config_file_applies_pair_groups(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """YAML defaults should support named instrument groups."""
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        """
histdatacom:
  validate_urls: true
  instrument_groups: [majors]
  formats: [ascii]
  timeframes: [tick-data-quotes]
  start_yearmonth: 2022-10
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--config", str(config_path)],
    )

    options = ArgParser(Options())()

    assert set(options.pairs) == {
        "audusd",
        "eurusd",
        "gbpusd",
        "nzdusd",
        "usdcad",
        "usdchf",
        "usdjpy",
    }
    assert options.pair_groups == ["majors"]


def test_config_file_applies_random_window_and_seed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Recurrent YAML requests should round-trip deterministic window inputs."""
    config_path = tmp_path / "random-window.yaml"
    config_path.write_text(
        """
histdatacom:
  validate_urls: true
  pairs: [eurusd]
  formats: [ascii]
  timeframes: [tick-data-quotes]
  random_window: 2d
  random_seed: 1729
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--config", str(config_path)],
    )

    options = ArgParser(Options())()

    assert options.random_window == "2d"
    assert options.random_seed == 1729


def test_bounded_session_window_accepts_same_month_without_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both bounds mean all session occurrences, including within one month."""
    monkeypatch.setattr(
        sys,
        "argv",
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
            "2024-01",
            "-e",
            "2024-01",
            "-r",
            "ldn",
        ],
    )

    options = ArgParser(Options())()

    assert options.start_yearmonth == "202401"
    assert options.end_yearmonth == "202401"
    assert options.random_window == "ldn"
    assert options.random_seed is None


@pytest.mark.parametrize(
    "tail",
    (
        ("--random-window", "2d"),
        ("--random-seed", "4"),
        ("--random-window", "ldn--ny", "--random-seed", "4"),
        ("-A", "--random-window", "2d", "--random-seed", "4"),
    ),
)
def test_random_window_cli_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tail: tuple[str, ...],
) -> None:
    """Missing seeds, orphan seeds, malformed forms, and repo mode must fail."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "-V",
            "-p",
            "eurusd",
            "-f",
            "ascii",
            "-t",
            "tick-data-quotes",
            *tail,
        ],
    )

    with pytest.raises(SystemExit) as err:
        ArgParser(Options())()

    assert err.value.code == 1


def test_config_file_applies_major_triangle_pair_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """YAML defaults should accept quoted human-readable group names."""
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        """
histdatacom:
  instrument_groups:
    - major triangles
  formats: [ascii]
  timeframes: [tick-data-quotes]
  start_yearmonth: 2022-10
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--config", str(config_path)],
    )

    options = ArgParser(Options())()

    assert tuple(sorted(options.pairs)) == MAJOR_TRIANGLE_SYMBOLS
    assert options.pair_groups == ["major-triangles"]


def test_config_file_applies_individual_triangle_pair_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """YAML defaults should accept individual triangle group names."""
    group = "triangle-eurgbp-eurusd-gbpusd"
    config_path = tmp_path / "histdatacom.yaml"
    config_path.write_text(
        f"""
histdatacom:
  instrument_groups:
    - {group}
  formats: [ascii]
  timeframes: [tick-data-quotes]
  start_yearmonth: 2022-10
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--config", str(config_path)],
    )

    options = ArgParser(Options())()

    assert tuple(sorted(options.pairs)) == tuple(
        sorted(MAJOR_TRIANGLE_PAIR_GROUPS[group])
    )
    assert options.pair_groups == [group]


def test_config_file_applies_quality_command_defaults(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """YAML config should support data-quality command options too."""
    config_path = tmp_path / "quality.yaml"
    report_path = tmp_path / "reports" / "quality.json"
    preview_path = tmp_path / "reports" / "quality-preview.txt"
    config_path.write_text(
        f"""
histdatacom:
  quality: true
  data_directory: {tmp_path}
  quality_checks: [inventory, ingestion]
  quality_report: {report_path}
  quality_preflight_evidence: {tmp_path / "preflight.json"}
  quality_preflight_evidence_max_age: 120
  quality_preflight_evidence_stale_ok: true
  quality_profile_preview: true
  quality_profile_preview_format: text
  quality_profile_preview_output: {preview_path}
  remediation_catalog_audit: true
  quality_fail_on: never
  quality_max_errors: 2
  quality_max_warnings: 5
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--config", str(config_path)],
    )

    options = ArgParser(Options())()

    assert options.data_quality
    assert options.quality_paths == (str(tmp_path),)
    assert options.quality_check_groups == ["inventory", "ingestion"]
    assert options.quality_report_path == str(report_path)
    assert options.quality_preflight_evidence_path == str(
        tmp_path / "preflight.json"
    )
    assert options.quality_preflight_evidence_max_age_seconds == 120
    assert options.quality_preflight_evidence_allow_stale
    assert options.quality_profile_preview
    assert options.quality_profile_preview_format == "text"
    assert options.quality_profile_preview_output_path == str(preview_path)
    assert options.quality_profile["reporting"] == {
        "remediation_catalog_audit": {"enabled": True}
    }
    sources = {
        item["path"]: item
        for item in options.quality_profile_resolution[
            "effective_value_sources"
        ]
    }
    assert (
        sources["/reporting/remediation_catalog_audit/enabled"]["source"]
        == "yaml_config"
    )
    assert options.quality_fail_on == "never"
    assert options.quality_max_errors == 2
    assert options.quality_max_warnings == 5
    assert not options.validate_urls


def test_config_file_applies_quality_preflight_defaults(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """YAML config should support cache-scale quality preflight options."""
    config_path = tmp_path / "quality-preflight.yaml"
    report_path = tmp_path / "reports" / "preflight.json"
    preview_path = tmp_path / "reports" / "preflight-profile.md"
    validation_path = tmp_path / "reports" / "validation.json"
    config_path.write_text(
        f"""
histdatacom:
  quality_preflight: true
  data_directory: {tmp_path}
  quality_checks: [ticks]
  quality_preflight_report: {report_path}
  quality_preflight_profile_preview_output: {preview_path}
  quality_preflight_profile_preview_format: markdown
  quality_preflight_validation_report: latest
  quality_preflight_validation_evidence: {validation_path}
  quality_preflight_run_validation: true
  quality_preflight_sample_size: 2
  pair_groups: [majors]
  formats: [ascii]
  timeframes: [tick-data-quotes]
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--config", str(config_path)],
    )

    options = ArgParser(Options())()

    assert options.quality_preflight
    assert options.quality_paths == (str(tmp_path),)
    assert options.quality_check_groups == ["ticks"]
    assert options.quality_preflight_report_path == str(report_path)
    assert options.quality_preflight_profile_preview_output_path == str(
        preview_path
    )
    assert options.quality_preflight_profile_preview_format == "markdown"
    assert options.quality_preflight_validation_report_path == "latest"
    assert options.quality_preflight_validation_evidence_path == str(
        validation_path
    )
    assert options.quality_preflight_run_validation
    assert options.quality_preflight_sample_size == 2
    assert options.pair_groups == ["majors"]
    assert not options.validate_urls


def test_config_file_rejects_unknown_options(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Config typos should fail instead of being silently ignored."""
    config_path = tmp_path / "bad.yaml"
    config_path.write_text(
        """
histdatacom:
  pairs: [eurusd]
  not_a_real_option: true
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["histdatacom", "--config", str(config_path)],
    )

    with pytest.raises(SystemExit) as err:
        ArgParser(Options())()

    assert err.value.code == 1


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
    options.orchestration_keep_runtime = True
    options.orchestration_wait_result = False
    options.no_overlap = True
    options.schedule_key = "eurusd-cache"

    assert options.use_orchestration
    assert not options.orchestration_start
    assert options.orchestration_keep_runtime
    assert not options.orchestration_wait_result
    assert options.no_overlap
    assert options.schedule_key == "eurusd-cache"


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
            "--quality-preflight-evidence",
            str(tmp_path / "preflight.json"),
            "--quality-preflight-evidence-max-age-seconds",
            "120",
            "--quality-preflight-evidence-stale-ok",
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
    assert options.quality_preflight_evidence_path == str(
        tmp_path / "preflight.json"
    )
    assert options.quality_preflight_evidence_max_age_seconds == 120
    assert options.quality_preflight_evidence_allow_stale
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
            "--quality-profile-preview",
            "--quality-profile-preview-format",
            "text",
            "--quality-profile-preview-output",
            str(tmp_path / "preview.txt"),
        ],
    )

    options = ArgParser(Options())()

    assert options.quality_profile_preview
    assert options.quality_profile_preview_format == "text"
    assert options.quality_profile_preview_output_path == str(
        tmp_path / "preview.txt"
    )
    assert options.quality_profile_path == str(profile_path)
    assert options.quality_profile["name"] == "cli-profile"
    assert options.quality_profile["source"] == "file"
    assert options.quality_profile["source_path"] == str(profile_path)
    resolution = options.quality_profile_resolution
    assert [channel["kind"] for channel in resolution["input_channels"]] == [
        "built_in_default",
        "named_profile",
        "profile_file",
    ]
    sources = {
        item["path"]: item for item in resolution["effective_value_sources"]
    }
    assert (
        sources["/rules/ingestion.ascii.row_count/min_row_count"]["source"]
        == "profile_file"
    )


def test_data_quality_cli_enables_remediation_catalog_audit_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The audit flag should materialize as validated profile reporting."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--quality-target",
            str(tmp_path),
            "--quality-remediation-catalog-audit",
        ],
    )

    options = ArgParser(Options())()

    assert options.quality_remediation_catalog_audit
    assert options.quality_profile["source"] == "cli-options"
    assert options.quality_profile["reporting"] == {
        "remediation_catalog_audit": {"enabled": True}
    }
    sources = {
        item["path"]: item
        for item in options.quality_profile_resolution[
            "effective_value_sources"
        ]
    }
    assert sources["/reporting/remediation_catalog_audit/enabled"] == {
        "path": "/reporting/remediation_catalog_audit/enabled",
        "value": True,
        "source": "cli_override",
        "profile_name": "operator",
        "override": True,
        "previous_source": "built_in_default",
        "overridden_source": "built_in_default",
        "previous_value": False,
    }


def test_data_quality_cli_merges_remediation_catalog_audit_with_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """CLI reporting opt-in should preserve unrelated profile settings."""
    profile_path = tmp_path / "quality-profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "name": "audit-merge",
                "reporting": {
                    "remediation_catalog_audit": {
                        "enabled": False,
                    }
                },
                "modeling_assumptions": {
                    "target_horizon_minutes": 5,
                },
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
            "--quality-profile-preview",
            "--quality-remediation-catalog-audit",
        ],
    )

    options = ArgParser(Options())()

    assert options.quality_profile_preview
    assert options.quality_profile["name"] == "audit-merge"
    assert options.quality_profile["source"] == "file"
    assert options.quality_profile["source_path"] == str(profile_path)
    assert options.quality_profile["modeling_assumptions"] == {
        "target_horizon_minutes": 5
    }
    assert options.quality_profile["reporting"] == {
        "remediation_catalog_audit": {"enabled": True}
    }
    sources = {
        item["path"]: item
        for item in options.quality_profile_resolution[
            "effective_value_sources"
        ]
    }
    assert (
        sources["/reporting/remediation_catalog_audit/enabled"]["source"]
        == "cli_override"
    )


def test_quality_profile_preview_uses_normal_profile_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Preview mode should fail with the same invalid-profile errors."""
    profile_path = tmp_path / "bad-quality-profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
                "rules": {"not.a.real.rule": {"warning_severity": "error"}},
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
            "--quality-profile-preview",
            "--quality-profile",
            str(profile_path),
        ],
    )

    with pytest.raises(SystemExit) as err:
        ArgParser(Options())()

    assert err.value.code == 1


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


def test_data_quality_cli_accepts_fingerprint_check_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The fingerprint quality group should be a first-class CLI choice."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "histdatacom",
            "--quality",
            "--quality-target",
            str(tmp_path),
            "--quality-checks",
            "fingerprint",
        ],
    )

    options = ArgParser(Options())()

    assert options.data_quality
    assert options.quality_check_groups == ["fingerprint"]


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
        ["histdatacom", "--quality-profile-preview"],
        ["histdatacom", "--quality-profile-preview-format", "text"],
        ["histdatacom", "--quality-profile-preview-output", "preview.md"],
        [
            "histdatacom",
            "--quality",
            "--quality-profile-preview-format",
            "text",
        ],
        [
            "histdatacom",
            "--quality",
            "--quality-profile-preview-output",
            "preview.md",
        ],
        ["histdatacom", "--quality-remediation-catalog-audit"],
        ["histdatacom", "--quality-preflight-evidence", "preflight.json"],
        ["histdatacom", "--quality-preflight-evidence-max-age-seconds", "60"],
        ["histdatacom", "--quality-preflight-evidence-stale-ok"],
        [
            "histdatacom",
            "--quality-preflight-validation-evidence",
            "validation.json",
        ],
        ["histdatacom", "--quality", "--quality-preflight-evidence-stale-ok"],
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
    options.quality_preflight_evidence_path = str(tmp_path / "preflight.json")
    options.quality_preflight_evidence_max_age_seconds = 120
    options.quality_preflight_evidence_allow_stale = True
    options.quality_remediation_catalog_audit = True
    options.quality_profile_preview = True
    options.quality_profile_preview_format = "markdown"
    options.quality_profile_preview_output_path = str(
        tmp_path / "api-preview.md"
    )
    options.quality_profile = {
        "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
        "name": "api-profile",
        "modeling_assumptions": {"target_horizon_minutes": 5},
    }

    parsed = ArgParser(options)()

    assert parsed.data_quality is True
    assert parsed.quality_profile_preview is True
    assert parsed.quality_profile_preview_format == "markdown"
    assert parsed.quality_profile_preview_output_path == str(
        tmp_path / "api-preview.md"
    )
    assert parsed.quality_preflight_evidence_path == str(
        tmp_path / "preflight.json"
    )
    assert parsed.quality_preflight_evidence_max_age_seconds == 120
    assert parsed.quality_preflight_evidence_allow_stale
    assert parsed.quality_profile["name"] == "api-profile"
    assert parsed.quality_profile["source"] == "api-options"
    assert parsed.quality_profile["modeling_assumptions"] == {
        "target_horizon_minutes": 5
    }
    assert parsed.quality_profile["reporting"] == {
        "remediation_catalog_audit": {"enabled": True}
    }
    assert [
        channel["kind"]
        for channel in parsed.quality_profile_resolution["input_channels"]
    ] == ["built_in_default", "named_profile", "api_options"]
    sources = {
        item["path"]: item
        for item in parsed.quality_profile_resolution["effective_value_sources"]
    }
    assert (
        sources["/reporting/remediation_catalog_audit/enabled"]["source"]
        == "api_options"
    )


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
            "tick-data-quotes",
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
    assert first_options.timeframes == ["T"]
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

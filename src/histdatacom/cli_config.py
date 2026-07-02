"""YAML-backed CLI configuration expansion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
import re

import yaml


class CliConfigError(ValueError):
    """Raised when a CLI configuration file is malformed."""


_ROOT_KEY = "histdatacom"
_CONFIG_KEYS = {"config", "config_path"}
_SECTION_ALIASES = {"orchestration_worker": "worker"}
_COMMAND_SECTION_KEYS = {
    "analytics",
    "cleanup",
    "jobs",
    "quality",
    "runtime",
    "worker",
}
_KEY_ALIASES = {
    "data_dir": "data_directory",
    "quality": "data_quality",
    "quality_profile": "quality_profile_path",
    "quality_path": "quality_paths",
    "quality_paths": "quality_paths",
    "quality_report": "quality_report_path",
    "quality_preflight_report": "quality_preflight_report_path",
    "quality_preflight_markdown": "quality_preflight_markdown",
    "quality_preflight_markdown_report": (
        "quality_preflight_markdown_report_path"
    ),
    "quality_preflight_markdown_report_path": (
        "quality_preflight_markdown_report_path"
    ),
    "quality_preflight_run_validation": "quality_preflight_run_validation",
    "quality_preflight_sample": "quality_preflight_sample_size",
    "quality_preflight_samples": "quality_preflight_sample_size",
    "quality_preflight_validation_report": (
        "quality_preflight_validation_report_path"
    ),
    "quality_preflight_validation_report_path": (
        "quality_preflight_validation_report_path"
    ),
    "quality_target": "quality_paths",
    "quality_targets": "quality_paths",
    "quality_check_groups": "quality_check_groups",
    "quality_checks": "quality_check_groups",
    "quality_preflight_evidence": "quality_preflight_evidence_path",
    "quality_preflight_evidence_allow_stale": (
        "quality_preflight_evidence_allow_stale"
    ),
    "quality_preflight_evidence_max_age": (
        "quality_preflight_evidence_max_age_seconds"
    ),
    "quality_preflight_evidence_max_age_seconds": (
        "quality_preflight_evidence_max_age_seconds"
    ),
    "quality_preflight_evidence_path": "quality_preflight_evidence_path",
    "quality_preflight_evidence_stale_ok": (
        "quality_preflight_evidence_allow_stale"
    ),
    "instrument_group": "pair_groups",
    "instrument_groups": "pair_groups",
    "pair_group": "pair_groups",
    "symbol_group": "pair_groups",
    "symbol_groups": "pair_groups",
    "keep_runtime": "orchestration_keep_runtime",
    "repo_quality": "repo_quality_refresh",
    "schedule": "schedule_key",
    "verbose": "verbosity",
}
_COMMAND_KEY_ALIASES = {
    "analytics_command": "command",
    "job_command": "command",
    "jobs_command": "command",
    "runtime_command": "command",
    "subcommand": "command",
    "worker_command": "command",
}
_COMMON_SCALAR_ARGS = {
    "runtime_home": "--runtime-home",
    "state_dir": "--state-dir",
    "workspace": "--workspace",
}
_COMMON_TRUE_FLAG_ARGS = {
    "json": "--json",
}
_WORKER_FLEET_SCALAR_ARGS = {
    "cpu_utilization": "--cpu-utilization",
    "influx_workers": "--influx-workers",
    "namespace": "--namespace",
    "network_multiplier": "--network-multiplier",
    "orchestration_workers": "--orchestration-workers",
    "task_queue_prefix": "--task-queue-prefix",
}
_TRUE_FLAG_ARGS = {
    "available_remote_data": "--available_remote_data",
    "update_remote_data": "--update_remote_data",
    "version": "--version",
    "validate_urls": "--validate_urls",
    "download_data_archives": "--download_data_archives",
    "extract_csvs": "--extract_csvs",
    "build_cache": "--build-cache",
    "import_to_influxdb": "--import_to_influxdb",
    "delete_after_influx": "--delete_after_influx",
    "data_quality": "--quality",
    "repo_quality_refresh": "--repo-quality",
    "quality_preflight": "--quality-preflight",
    "quality_preflight_evidence_allow_stale": (
        "--quality-preflight-evidence-stale-ok"
    ),
    "quality_preflight_markdown": "--quality-preflight-markdown",
    "quality_preflight_run_validation": "--quality-preflight-run-validation",
    "repo_quality_columns": "--repo-quality-columns",
    "no_overlap": "--no-overlap",
}
_SCALAR_ARGS = {
    "by": "--by",
    "start_yearmonth": "--start_yearmonth",
    "end_yearmonth": "--end_yearmonth",
    "batch_size": "--batch_size",
    "cpu_utilization": "--cpu_utilization",
    "data_directory": "--data-directory",
    "quality_report_path": "--quality-report",
    "quality_preflight_evidence_path": "--quality-preflight-evidence",
    "quality_preflight_markdown_report_path": (
        "--quality-preflight-markdown-report"
    ),
    "quality_preflight_report_path": "--quality-preflight-report",
    "quality_preflight_validation_report_path": (
        "--quality-preflight-validation-report"
    ),
    "quality_preflight_sample_size": "--quality-preflight-sample-size",
    "quality_profile_path": "--quality-profile",
    "quality_fail_on": "--quality-fail-on",
    "quality_max_errors": "--quality-max-errors",
    "quality_max_warnings": "--quality-max-warnings",
    "quality_preflight_evidence_max_age_seconds": (
        "--quality-preflight-evidence-max-age-seconds"
    ),
    "schedule_key": "--schedule-key",
}
_LIST_ARGS = {
    "pair_groups": "--pair-groups",
    "pairs": "--pairs",
    "formats": "--formats",
    "timeframes": "--timeframes",
    "quality_paths": "--quality-target",
    "quality_check_groups": "--quality-checks",
}
_CONTROL_BOOL_KEYS = {
    "orchestration_keep_runtime",
    "orchestration_start",
    "orchestration_wait_result",
    "submit_only",
}
_ALLOWED_KEYS = (
    set(_TRUE_FLAG_ARGS)
    | set(_SCALAR_ARGS)
    | set(_LIST_ARGS)
    | _CONTROL_BOOL_KEYS
    | {"verbosity"}
)
_ANALYTICS_COMMANDS = {"feed-regimes"}
_ANALYTICS_ALIASES = {
    **_COMMAND_KEY_ALIASES,
    "path": "paths",
    "target": "paths",
    "targets": "paths",
}
_ANALYTICS_TRUE_FLAG_ARGS = {
    "json": "--json",
}
_ANALYTICS_SCALAR_ARGS = {
    "bucket": "--bucket",
    "quiet_gap_ms": "--quiet-gap-ms",
    "report": "--report",
}
_ANALYTICS_LIST_ARGS = {
    "paths": "--target",
}
_ANALYTICS_ALLOWED_KEYS = (
    {"command", "verbosity"}
    | set(_ANALYTICS_TRUE_FLAG_ARGS)
    | set(_ANALYTICS_SCALAR_ARGS)
    | set(_ANALYTICS_LIST_ARGS)
)
_CLEANUP_COMMANDS = {"sources", "status", "transient-sources"}
_CLEANUP_ALIASES = {
    **_COMMAND_KEY_ALIASES,
    "cleanup_command": "command",
    "instrument_group": "pair_groups",
    "instrument_groups": "pair_groups",
    "pair_group": "pair_groups",
    "symbol_group": "pair_groups",
    "symbol_groups": "pair_groups",
}
_CLEANUP_TRUE_FLAG_ARGS = {
    "apply": "--apply",
}
_CLEANUP_SCALAR_ARGS = {
    **_COMMON_SCALAR_ARGS,
    "data_directory": "--data-directory",
    "max_jobs": "--max-jobs",
}
_CLEANUP_LIST_ARGS = {
    "formats": "--formats",
    "pair_groups": "--pair-groups",
    "pairs": "--pairs",
    "timeframes": "--timeframes",
}
_CLEANUP_ALLOWED_KEYS = (
    {"command"}
    | set(_COMMON_TRUE_FLAG_ARGS)
    | set(_CLEANUP_TRUE_FLAG_ARGS)
    | set(_CLEANUP_SCALAR_ARGS)
    | set(_CLEANUP_LIST_ARGS)
)
_QUALITY_COMMANDS = {"doctor-evidence", "evidence", "inspect-evidence"}
_QUALITY_ALIASES = {
    **_COMMAND_KEY_ALIASES,
    "data_directory": "target_root",
    "evidence": "evidence_path",
    "instrument_group": "pair_groups",
    "instrument_groups": "pair_groups",
    "path": "target_root",
    "pair_group": "pair_groups",
    "preflight_evidence": "evidence_path",
    "quality_check_groups": "quality_check_groups",
    "quality_checks": "quality_check_groups",
    "quality_path": "target_root",
    "quality_preflight_evidence": "evidence_path",
    "quality_preflight_evidence_max_age": ("evidence_max_age_seconds"),
    "quality_preflight_evidence_max_age_seconds": ("evidence_max_age_seconds"),
    "quality_preflight_evidence_path": "evidence_path",
    "quality_preflight_evidence_stale_ok": "allow_stale_evidence",
    "quality_target": "target_root",
    "symbol_group": "pair_groups",
    "symbol_groups": "pair_groups",
    "target": "target_root",
}
_QUALITY_TRUE_FLAG_ARGS = {
    "allow_stale_evidence": "--quality-preflight-evidence-stale-ok",
    "json": "--json",
}
_QUALITY_SCALAR_ARGS = {
    "evidence_max_age_seconds": (
        "--quality-preflight-evidence-max-age-seconds"
    ),
    "evidence_path": "--evidence",
    "target_root": "--target",
}
_QUALITY_LIST_ARGS = {
    "formats": "--formats",
    "pair_groups": "--pair-groups",
    "pairs": "--pairs",
    "quality_check_groups": "--quality-checks",
    "timeframes": "--timeframes",
}
_QUALITY_ALLOWED_KEYS = (
    {"command", "verbosity"}
    | set(_COMMON_TRUE_FLAG_ARGS)
    | set(_QUALITY_TRUE_FLAG_ARGS)
    | set(_QUALITY_SCALAR_ARGS)
    | set(_QUALITY_LIST_ARGS)
)
_RUNTIME_COMMANDS = {
    "cleanup",
    "doctor",
    "jobs",
    "maintenance",
    "restart",
    "start",
    "status",
    "stop",
}
_RUNTIME_ALIASES = {
    **_COMMAND_KEY_ALIASES,
    "extra_arg": "extra_args",
    "temporal_args": "extra_args",
}
_RUNTIME_TRUE_FLAG_ARGS = {
    "allow_running": "--allow-running",
}
_RUNTIME_SCALAR_ARGS = {
    **_WORKER_FLEET_SCALAR_ARGS,
    "executable": "--executable",
    "max_artifacts_per_owner": "--max-artifacts-per-owner",
    "max_dataset_plans_per_request": "--max-dataset-plans-per-request",
    "max_job_snapshots": "--max-job-snapshots",
    "max_log_bytes": "--max-log-bytes",
    "max_rotated_logs": "--max-rotated-logs",
    "max_stage_results_per_work_item": ("--max-stage-results-per-work-item"),
    "max_status_events_per_owner": "--max-status-events-per-owner",
    "max_temporal_sqlite_bytes": "--max-temporal-sqlite-bytes",
    "startup_timeout": "--startup-timeout",
    "stop_timeout": "--stop-timeout",
}
_RUNTIME_ALLOWED_KEYS = (
    {"command", "extra_args", "jobs"}
    | set(_COMMON_TRUE_FLAG_ARGS)
    | set(_COMMON_SCALAR_ARGS)
    | set(_RUNTIME_TRUE_FLAG_ARGS)
    | set(_RUNTIME_SCALAR_ARGS)
)
_JOBS_COMMANDS = {
    "artifacts",
    "cancel",
    "inspect",
    "list",
    "logs",
    "preflight",
    "progress",
    "result",
    "resume",
    "retry",
    "submit",
}
_JOBS_ALIASES = {
    **_COMMAND_KEY_ALIASES,
    "request": "request_json",
}
_JOBS_TRUE_FLAG_ARGS = {
    "active": "--active",
    "keep_runtime": "--keep-runtime",
    "no_overlap": "--no-overlap",
    "offline": "--offline",
    "recompute_complete": "--recompute-complete",
    "start": "--start",
    "submit_only": "--submit-only",
}
_JOBS_SCALAR_ARGS = {
    "limit": "--limit",
    "query": "--query",
    "reason": "--reason",
    "request_json": "--request-json",
    "run_id": "--run-id",
    "schedule_fingerprint": "--schedule-fingerprint",
    "schedule_key": "--schedule-key",
}
_JOBS_ALLOWED_KEYS = (
    {"command", "workflow_id"}
    | set(_COMMON_TRUE_FLAG_ARGS)
    | set(_COMMON_SCALAR_ARGS)
    | set(_JOBS_TRUE_FLAG_ARGS)
    | set(_JOBS_SCALAR_ARGS)
)
_JOBS_IDENTITY_COMMANDS = {
    "artifacts",
    "cancel",
    "inspect",
    "logs",
    "progress",
    "result",
    "resume",
    "retry",
}
_JOBS_REASON_COMMANDS = {"cancel", "resume", "retry"}
_JOBS_RECOMPUTE_COMMANDS = {"resume", "retry"}
_WORKER_COMMANDS = {"config", "run"}
_WORKER_ALIASES = {
    **_COMMAND_KEY_ALIASES,
    "activity_workers": "max_concurrent_activities",
}
_WORKER_SCALAR_ARGS = {
    **_WORKER_FLEET_SCALAR_ARGS,
    "lane": "--lane",
    "max_concurrent_activities": "--max-concurrent-activities",
}
_WORKER_ALLOWED_KEYS = (
    {"command"}
    | set(_COMMON_TRUE_FLAG_ARGS)
    | set(_COMMON_SCALAR_ARGS)
    | set(_WORKER_SCALAR_ARGS)
)


def add_config_argument(parser: Any) -> None:
    """Add the shared ``--config`` option to an argparse parser."""
    parser.add_argument(
        "--config",
        dest="config_path",
        type=str,
        metavar="PATH",
        help=(
            "read recurrent-run defaults from a YAML file; explicit CLI "
            "flags override configured values"
        ),
    )


def config_path_from_cli_args(args: Sequence[str]) -> str:
    """Return a ``--config`` path from raw CLI args, when present."""
    for index, arg in enumerate(args):
        if arg == "--config":
            try:
                return args[index + 1]
            except IndexError as exc:
                raise CliConfigError("--config requires a path") from exc
        if arg.startswith("--config="):
            return arg.partition("=")[2]
    return ""


def strip_config_option(args: Sequence[str]) -> list[str]:
    """Return CLI args with ``--config`` and its path removed."""
    stripped: list[str] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--config":
            if index + 1 >= len(args):
                raise CliConfigError("--config requires a path")
            index += 2
            continue
        if arg.startswith("--config="):
            index += 1
            continue
        stripped.append(arg)
        index += 1
    return stripped


def routed_command_from_cli_args(
    args: Sequence[str],
    commands: set[str],
) -> str:
    """Return the first non-config routed command token, when present."""
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--config":
            if index + 1 >= len(args):
                return ""
            index += 2
            continue
        if arg.startswith("--config="):
            index += 1
            continue
        return arg if arg in commands else ""
    return ""


def remove_routed_command_from_cli_args(
    args: Sequence[str],
    command: str,
) -> list[str]:
    """Remove the routed command token while preserving all other args."""
    stripped_index = 0
    output: list[str] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--config":
            output.extend(args[index : index + 2])
            index += 2
            continue
        if arg.startswith("--config="):
            output.append(arg)
            index += 1
            continue
        if stripped_index == 0 and arg == command:
            stripped_index += 1
            index += 1
            continue
        output.append(arg)
        stripped_index += 1
        index += 1
    return output


def load_cli_config_args(
    config_path: str,
    *,
    cli_args: Sequence[str] = (),
) -> list[str]:
    """Load a YAML config file and return equivalent primary CLI arguments."""
    if not config_path:
        return []
    config = _normalized_config_mapping(_load_config_mapping(config_path))
    args: list[str] = []
    for key in sorted(config):
        value = config[key]
        if key in _TRUE_FLAG_ARGS:
            if _bool_value(key, value):
                args.append(_TRUE_FLAG_ARGS[key])
            continue
        if key in _SCALAR_ARGS:
            args.extend(_scalar_arg(_SCALAR_ARGS[key], value))
            continue
        if key in _LIST_ARGS:
            args.extend(_list_arg(_LIST_ARGS[key], value))
            continue
        if key == "verbosity":
            if not _has_cli_verbosity(cli_args):
                args.extend(_verbosity_arg(value))
            continue
        args.extend(_control_bool_arg(key, value))
    return args


def configured_analytics_argv(args: Sequence[str]) -> list[str]:
    """Return analytics argv with YAML defaults injected."""
    return _configured_subcommand_argv(
        args,
        section_name="analytics",
        commands=_ANALYTICS_COMMANDS,
        allowed_keys=_ANALYTICS_ALLOWED_KEYS,
        aliases=_ANALYTICS_ALIASES,
        global_true_flags={},
        global_scalar_args={},
        global_list_args={},
        command_true_flags=_ANALYTICS_TRUE_FLAG_ARGS,
        command_scalar_args=_ANALYTICS_SCALAR_ARGS,
        command_list_args=_ANALYTICS_LIST_ARGS,
    )


def configured_cleanup_argv(args: Sequence[str]) -> list[str]:
    """Return cleanup argv with YAML defaults injected."""
    return _configured_subcommand_argv(
        args,
        section_name="cleanup",
        commands=_CLEANUP_COMMANDS,
        allowed_keys=_CLEANUP_ALLOWED_KEYS,
        aliases=_CLEANUP_ALIASES,
        global_true_flags=_COMMON_TRUE_FLAG_ARGS,
        global_scalar_args={},
        global_list_args={},
        command_true_flags=_CLEANUP_TRUE_FLAG_ARGS,
        command_scalar_args=_CLEANUP_SCALAR_ARGS,
        command_list_args=_CLEANUP_LIST_ARGS,
    )


def configured_quality_argv(args: Sequence[str]) -> list[str]:
    """Return quality argv with YAML defaults injected."""
    return _configured_subcommand_argv(
        args,
        section_name="quality",
        commands=_QUALITY_COMMANDS,
        allowed_keys=_QUALITY_ALLOWED_KEYS,
        aliases=_QUALITY_ALIASES,
        global_true_flags={},
        global_scalar_args={},
        global_list_args={},
        command_true_flags=_QUALITY_TRUE_FLAG_ARGS,
        command_scalar_args=_QUALITY_SCALAR_ARGS,
        command_list_args=_QUALITY_LIST_ARGS,
    )


def configured_runtime_argv(args: Sequence[str]) -> list[str]:
    """Return runtime argv with YAML defaults injected."""
    config_path = config_path_from_cli_args(args)
    explicit_args = strip_config_option(args)
    if not config_path:
        return explicit_args
    config = _normalized_section_mapping(
        _section_mapping(config_path, "runtime"),
        allowed_keys=_RUNTIME_ALLOWED_KEYS,
        aliases=_RUNTIME_ALIASES,
        section_name="runtime",
    )
    prefix, explicit_command, suffix = _split_at_command(
        explicit_args,
        _RUNTIME_COMMANDS,
    )
    configured_command = _command_from_config(
        config,
        section_name="runtime",
        commands=_RUNTIME_COMMANDS,
    )
    command = explicit_command or configured_command
    global_args = _mapped_args(
        config,
        true_flags=_COMMON_TRUE_FLAG_ARGS,
        scalar_args=_COMMON_SCALAR_ARGS,
        list_args={},
    )
    command_args = (
        _runtime_command_args(config, command, suffix)
        if _include_command_defaults(configured_command, command)
        else []
    )
    if command:
        return [
            *global_args,
            *prefix,
            command,
            *command_args,
            *suffix,
        ]
    return [*global_args, *explicit_args]


def configured_jobs_argv(args: Sequence[str]) -> list[str]:
    """Return jobs argv with YAML defaults injected."""
    config_path = config_path_from_cli_args(args)
    explicit_args = strip_config_option(args)
    if not config_path:
        return explicit_args
    config = _normalized_section_mapping(
        _section_mapping(config_path, "jobs"),
        allowed_keys=_JOBS_ALLOWED_KEYS,
        aliases=_JOBS_ALIASES,
        section_name="jobs",
    )
    prefix, explicit_command, suffix = _split_at_command(
        explicit_args,
        _JOBS_COMMANDS,
    )
    configured_command = _command_from_config(
        config,
        section_name="jobs",
        commands=_JOBS_COMMANDS,
    )
    command = explicit_command or configured_command
    global_args = _mapped_args(
        config,
        true_flags={
            "json": _COMMON_TRUE_FLAG_ARGS["json"],
            "offline": _JOBS_TRUE_FLAG_ARGS["offline"],
        },
        scalar_args=_COMMON_SCALAR_ARGS,
        list_args={},
    )
    command_args = (
        _jobs_command_args(config, command, suffix)
        if _include_command_defaults(configured_command, command)
        else []
    )
    if command:
        return [
            *global_args,
            *prefix,
            command,
            *command_args,
            *suffix,
        ]
    return [*global_args, *explicit_args]


def configured_worker_argv(args: Sequence[str]) -> list[str]:
    """Return orchestration-worker argv with YAML defaults injected."""
    return _configured_subcommand_argv(
        args,
        section_name="worker",
        commands=_WORKER_COMMANDS,
        allowed_keys=_WORKER_ALLOWED_KEYS,
        aliases=_WORKER_ALIASES,
        global_true_flags=_COMMON_TRUE_FLAG_ARGS,
        global_scalar_args=_COMMON_SCALAR_ARGS,
        global_list_args={},
        command_true_flags={},
        command_scalar_args=_WORKER_SCALAR_ARGS,
        command_list_args={},
    )


def _configured_subcommand_argv(
    args: Sequence[str],
    *,
    section_name: str,
    commands: set[str],
    allowed_keys: set[str],
    aliases: Mapping[str, str],
    global_true_flags: Mapping[str, str],
    global_scalar_args: Mapping[str, str],
    global_list_args: Mapping[str, str],
    command_true_flags: Mapping[str, str],
    command_scalar_args: Mapping[str, str],
    command_list_args: Mapping[str, str],
) -> list[str]:
    config_path = config_path_from_cli_args(args)
    explicit_args = strip_config_option(args)
    if not config_path:
        return explicit_args
    config = _normalized_section_mapping(
        _section_mapping(config_path, section_name),
        allowed_keys=allowed_keys,
        aliases=aliases,
        section_name=section_name,
    )
    prefix, explicit_command, suffix = _split_at_command(
        explicit_args,
        commands,
    )
    configured_command = _command_from_config(
        config,
        section_name=section_name,
        commands=commands,
    )
    command = explicit_command or configured_command
    global_args = _mapped_args(
        config,
        true_flags=global_true_flags,
        scalar_args=global_scalar_args,
        list_args=global_list_args,
    )
    if "verbosity" in config and not _has_cli_verbosity(explicit_args):
        global_args.extend(_verbosity_arg(config["verbosity"]))
    command_args = (
        _mapped_args(
            config,
            true_flags=command_true_flags,
            scalar_args=command_scalar_args,
            list_args=command_list_args,
        )
        if _include_command_defaults(configured_command, command)
        else []
    )
    if command:
        return [
            *global_args,
            *prefix,
            command,
            *command_args,
            *suffix,
        ]
    return [*global_args, *explicit_args]


def _runtime_command_args(
    config: Mapping[str, Any],
    command: str,
    explicit_suffix: Sequence[str],
) -> list[str]:
    if command == "jobs":
        return _runtime_jobs_command_args(config, explicit_suffix)
    args = _mapped_args(
        config,
        true_flags=_runtime_true_flags_for_command(command),
        scalar_args=_runtime_scalar_args_for_command(command),
        list_args={},
    )
    if command in {"start", "restart"} and "extra_args" in config:
        if "--" not in explicit_suffix:
            args.extend(
                ["--", *_list_values("extra_args", config["extra_args"])]
            )
    return args


def _runtime_jobs_command_args(
    config: Mapping[str, Any],
    explicit_suffix: Sequence[str],
) -> list[str]:
    raw_jobs_config = config.get("jobs")
    if raw_jobs_config is None:
        return []
    if not isinstance(raw_jobs_config, Mapping):
        raise CliConfigError("'runtime.jobs' config section must be a mapping")
    jobs_config = _normalized_section_mapping(
        raw_jobs_config,
        allowed_keys=_JOBS_ALLOWED_KEYS,
        aliases=_JOBS_ALIASES,
        section_name="runtime.jobs",
    )
    prefix, explicit_command, suffix = _split_at_command(
        explicit_suffix,
        _JOBS_COMMANDS,
    )
    configured_command = _command_from_config(
        jobs_config,
        section_name="runtime.jobs",
        commands=_JOBS_COMMANDS,
    )
    command = explicit_command or configured_command
    global_args = _mapped_args(
        jobs_config,
        true_flags={
            "json": _COMMON_TRUE_FLAG_ARGS["json"],
            "offline": _JOBS_TRUE_FLAG_ARGS["offline"],
        },
        scalar_args={},
        list_args={},
    )
    command_args = (
        _jobs_command_args(jobs_config, command, suffix)
        if _include_command_defaults(configured_command, command)
        else []
    )
    if command:
        return [*global_args, *prefix, command, *command_args, *suffix]
    return [*global_args, *explicit_suffix]


def _runtime_true_flags_for_command(command: str) -> Mapping[str, str]:
    if command in {"cleanup", "maintenance"}:
        return _RUNTIME_TRUE_FLAG_ARGS
    return {}


def _runtime_scalar_args_for_command(command: str) -> Mapping[str, str]:
    worker_args = {
        key: _RUNTIME_SCALAR_ARGS[key] for key in _WORKER_FLEET_SCALAR_ARGS
    }
    if command == "start":
        return {
            **worker_args,
            "executable": "--executable",
            "startup_timeout": "--startup-timeout",
        }
    if command == "restart":
        return {
            **worker_args,
            "executable": "--executable",
            "startup_timeout": "--startup-timeout",
            "stop_timeout": "--stop-timeout",
        }
    if command == "stop":
        return {"stop_timeout": "--stop-timeout"}
    if command in {"cleanup", "maintenance"}:
        maintenance_keys = {
            "max_artifacts_per_owner",
            "max_dataset_plans_per_request",
            "max_job_snapshots",
            "max_log_bytes",
            "max_rotated_logs",
            "max_stage_results_per_work_item",
            "max_status_events_per_owner",
            "max_temporal_sqlite_bytes",
        }
        return {key: _RUNTIME_SCALAR_ARGS[key] for key in maintenance_keys}
    return {}


def _jobs_command_args(
    config: Mapping[str, Any],
    command: str,
    explicit_suffix: Sequence[str],
) -> list[str]:
    args: list[str] = []
    if (
        command in _JOBS_IDENTITY_COMMANDS
        and "workflow_id" in config
        and not _contains_positional(
            explicit_suffix,
            value_flags={"--reason", "--run-id"},
        )
    ):
        args.append(str(config["workflow_id"]))
    true_flags: dict[str, str] = {}
    scalar_args: dict[str, str] = {}
    if command in {"preflight", "submit"}:
        true_flags.update(
            {
                "no_overlap": _JOBS_TRUE_FLAG_ARGS["no_overlap"],
            }
        )
        if command == "submit":
            true_flags.update(
                {
                    "start": _JOBS_TRUE_FLAG_ARGS["start"],
                    "submit_only": _JOBS_TRUE_FLAG_ARGS["submit_only"],
                }
            )
        scalar_args["request_json"] = _JOBS_SCALAR_ARGS["request_json"]
        scalar_args["schedule_key"] = _JOBS_SCALAR_ARGS["schedule_key"]
    elif command == "list":
        true_flags["active"] = _JOBS_TRUE_FLAG_ARGS["active"]
        scalar_args.update(
            {
                "limit": _JOBS_SCALAR_ARGS["limit"],
                "query": _JOBS_SCALAR_ARGS["query"],
                "schedule_fingerprint": _JOBS_SCALAR_ARGS[
                    "schedule_fingerprint"
                ],
                "schedule_key": _JOBS_SCALAR_ARGS["schedule_key"],
            }
        )
    elif command in _JOBS_IDENTITY_COMMANDS:
        scalar_args["run_id"] = _JOBS_SCALAR_ARGS["run_id"]
        if command in _JOBS_REASON_COMMANDS:
            scalar_args["reason"] = _JOBS_SCALAR_ARGS["reason"]
        if command in _JOBS_RECOMPUTE_COMMANDS:
            true_flags["recompute_complete"] = _JOBS_TRUE_FLAG_ARGS[
                "recompute_complete"
            ]
    args.extend(
        _mapped_args(
            config,
            true_flags=true_flags,
            scalar_args=scalar_args,
            list_args={},
        )
    )
    return args


def _load_config_mapping(config_path: str) -> Mapping[str, Any]:
    path = Path(config_path).expanduser()
    if not path.exists():
        raise CliConfigError(f"config file does not exist: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise CliConfigError(f"config file is not valid YAML: {path}") from exc

    if loaded is None:
        return {}
    if not isinstance(loaded, Mapping):
        raise CliConfigError("config file must contain a mapping")
    root = loaded.get(_ROOT_KEY)
    if root is None:
        return loaded
    if not isinstance(root, Mapping):
        raise CliConfigError(f"{_ROOT_KEY!r} config section must be a mapping")
    return root


def _section_mapping(
    config_path: str,
    section_name: str,
) -> Mapping[str, Any]:
    root = _load_config_mapping(config_path)
    for raw_key, value in root.items():
        key = _normalize_section_key(raw_key)
        if key == section_name:
            if isinstance(value, Mapping):
                return value
            continue
    has_sections = any(
        _normalize_section_key(raw_key) in _COMMAND_SECTION_KEYS
        and isinstance(value, Mapping)
        for raw_key, value in root.items()
    )
    return {} if has_sections else root


def _normalized_config_mapping(
    mapping: Mapping[str, Any],
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    unknown: list[str] = []
    for raw_key, value in mapping.items():
        key = _normalize_key(raw_key)
        section_key = _normalize_section_key(raw_key)
        if section_key in _COMMAND_SECTION_KEYS and isinstance(value, Mapping):
            continue
        if key in _CONFIG_KEYS:
            raise CliConfigError(
                "--config cannot be nested inside a config file"
            )
        if key not in _ALLOWED_KEYS:
            unknown.append(str(raw_key))
            continue
        if key in normalized:
            raise CliConfigError(
                f"duplicate config key after normalization: {key}"
            )
        normalized[key] = value
    if unknown:
        raise CliConfigError(
            "unsupported config option(s): " + ", ".join(sorted(unknown))
        )
    return normalized


def _normalized_section_mapping(
    mapping: Mapping[str, Any],
    *,
    allowed_keys: set[str],
    aliases: Mapping[str, str],
    section_name: str,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    unknown: list[str] = []
    for raw_key, value in mapping.items():
        key = _normalize_key(raw_key, aliases=aliases)
        if key in _CONFIG_KEYS:
            raise CliConfigError(
                "--config cannot be nested inside a config file"
            )
        if key not in allowed_keys:
            unknown.append(str(raw_key))
            continue
        if key in normalized:
            raise CliConfigError(
                f"duplicate {section_name} config key after normalization: {key}"
            )
        normalized[key] = value
    if unknown:
        raise CliConfigError(
            f"unsupported {section_name} config option(s): "
            + ", ".join(sorted(unknown))
        )
    return normalized


def _normalize_key(
    key: object,
    *,
    aliases: Mapping[str, str] | None = None,
) -> str:
    normalized = str(key).strip().lstrip("-").replace("-", "_")
    if aliases and normalized in aliases:
        return aliases[normalized]
    return _KEY_ALIASES.get(normalized, normalized)


def _normalize_section_key(key: object) -> str:
    normalized = str(key).strip().lstrip("-").replace("-", "_")
    return _SECTION_ALIASES.get(normalized, normalized)


def _mapped_args(
    config: Mapping[str, Any],
    *,
    true_flags: Mapping[str, str],
    scalar_args: Mapping[str, str],
    list_args: Mapping[str, str],
) -> list[str]:
    args: list[str] = []
    for key in sorted(config):
        value = config[key]
        if key in true_flags:
            if _bool_value(key, value):
                args.append(true_flags[key])
            continue
        if key in scalar_args:
            args.extend(_scalar_arg(scalar_args[key], value))
            continue
        if key in list_args:
            args.extend(_list_arg(list_args[key], value))
    return args


def _command_from_config(
    config: Mapping[str, Any],
    *,
    section_name: str,
    commands: set[str],
) -> str:
    value = config.get("command")
    if value is None or value == "":
        return ""
    if isinstance(value, bool) or isinstance(value, Mapping):
        raise CliConfigError(f"{section_name} command must be a string")
    command = str(value).strip().replace("_", "-")
    if command not in commands:
        supported = ", ".join(sorted(commands))
        raise CliConfigError(
            f"{section_name} command must be one of: {supported}"
        )
    return command


def _include_command_defaults(
    configured_command: str,
    selected_command: str,
) -> bool:
    return bool(selected_command) and (
        not configured_command or configured_command == selected_command
    )


def _split_at_command(
    args: Sequence[str],
    commands: set[str],
) -> tuple[list[str], str, list[str]]:
    for index, arg in enumerate(args):
        if arg in commands:
            return list(args[:index]), arg, list(args[index + 1 :])
    return list(args), "", []


def _contains_positional(
    args: Sequence[str],
    *,
    value_flags: set[str],
) -> bool:
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg == "--":
            return True
        if arg in value_flags:
            skip_next = True
            continue
        if any(arg.startswith(f"{flag}=") for flag in value_flags):
            continue
        if arg.startswith("-"):
            continue
        return True
    return False


def _bool_value(key: str, value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise CliConfigError(f"{key} must be a boolean")


def _scalar_arg(flag: str, value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if (
        isinstance(value, bool)
        or isinstance(value, Mapping)
        or _is_sequence(value)
    ):
        raise CliConfigError(f"{flag} requires a scalar value")
    return [flag, str(value)]


def _list_arg(flag: str, value: Any) -> list[str]:
    if value is None or value == "":
        return []
    values = _list_values(flag, value)
    if not values:
        return []
    return [flag, *values]


def _list_values(key: str, value: Any) -> list[str]:
    if isinstance(value, str):
        values = [value]
    elif _is_sequence(value):
        values = list(value)
    else:
        raise CliConfigError(f"{key} requires a list or string value")
    if any(item is None or isinstance(item, bool) for item in values):
        raise CliConfigError(f"{key} list contains an invalid value")
    return [str(item) for item in values]


def _verbosity_arg(value: Any) -> list[str]:
    if isinstance(value, bool):
        count = 1 if value else 0
    else:
        try:
            count = int(value or 0)
        except (TypeError, ValueError) as exc:
            raise CliConfigError(
                "verbosity must be a non-negative integer"
            ) from exc
    if count < 0:
        raise CliConfigError("verbosity must be a non-negative integer")
    return ["-" + "v" * count] if count else []


def _control_bool_arg(key: str, value: Any) -> list[str]:
    enabled = _bool_value(key, value)
    if key == "orchestration_start":
        return [
            "--orchestration-start" if enabled else "--no-orchestration-start"
        ]
    if key == "orchestration_keep_runtime":
        return ["--keep-runtime" if enabled else "--no-keep-runtime"]
    if key == "orchestration_wait_result":
        return [] if enabled else ["--submit-only"]
    if key == "submit_only":
        return ["--submit-only"] if enabled else []
    raise CliConfigError(f"unsupported config option: {key}")


def _has_cli_verbosity(args: Sequence[str]) -> bool:
    return any(arg == "--verbose" or re.fullmatch(r"-v+", arg) for arg in args)


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value,
        str | bytes | bytearray,
    )

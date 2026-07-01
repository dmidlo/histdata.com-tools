"""Run main application. Core logic.

Raises:
    SystemExit: Exit when complete.

Returns:
    repo_data (set): a set of repo pairs with start and end date ranges.
    Data (PolarsDataFrame | DataFrame | Table):
        a Polars DataFrame, pandas DataFrame, or pyarrow Table
    List Of Data:   [
                        {
                            "timeframe": timeframe,
                            "pair": pair,
                            "records": [record, record, ...],
                            "data": PolarsDataFrame | DataFrame | Table,
                        },
                        ...
                        ...
                    ]

"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import sys
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping, TypeGuard

import histdatacom
from histdatacom import Options
from histdatacom.cli import ArgParser
from histdatacom.cli_config import (
    remove_routed_command_from_cli_args,
    routed_command_from_cli_args,
)
from histdatacom.exceptions import (
    format_exception_for_cli,
    format_failure_info_for_cli,
    InfluxConfigurationError,
)
from histdatacom.data_quality.preflight import (
    format_quality_preflight_console_summary,
    format_quality_run_preflight_warning,
    quality_run_preflight_warning,
    run_cache_quality_preflight,
    write_quality_preflight_report,
)
from histdatacom.fx_enums import expand_pair_selection
from histdatacom.repository_output import (
    print_repository_failure,
    print_repository_table,
)
from histdatacom.histdata_ascii import CACHE_FILENAME
from histdatacom.publication_safety import publish_safe_path
from histdatacom.records import Record
from histdatacom.runtime_contracts import FailureInfo, RunRequest, WorkStatus
from histdatacom.orchestration.client import (
    JobResult,
    OrchestrationUnavailableError,
    submit_run_request_and_observe_sync,
)
from histdatacom.orchestration.cutover import (
    FOREGROUND_RUNTIME_REMOVED_MESSAGE,
    should_submit_to_orchestration,
)
from histdatacom.orchestration.rich_progress import LiveJobProgressRenderer
from histdatacom.operational_health import (
    operational_health_provider_for_request,
)
from histdatacom.utils import (
    load_influx_yaml,
    set_working_data_dir,
    normalize_api_return_type,
)
from histdatacom.verbosity import configure_logging

if TYPE_CHECKING:
    from pandas import DataFrame
    from polars import DataFrame as PolarsDataFrame
    from pyarrow import Table


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    """Resolved launch context for orchestrated execution."""

    args: Mapping[str, Any]
    request: RunRequest
    version: bool
    from_api: bool
    orchestration_start: bool
    orchestration_keep_runtime: bool
    orchestration_wait_result: bool
    api_return_type: str | None
    data_quality: bool
    quality_paths: tuple[str, ...]
    quality_check_groups: tuple[str, ...]
    quality_report_path: str | None
    quality_fail_on: str
    quality_max_errors: int
    quality_max_warnings: int
    quality_preflight: bool
    quality_preflight_evidence_allow_stale: bool
    quality_preflight_evidence_max_age_seconds: int
    quality_preflight_evidence_path: str | None
    quality_preflight_report_path: str | None
    quality_preflight_sample_size: int
    quality_profile_path: str
    quality_profile: Mapping[str, Any]
    repo_quality_refresh: bool
    repo_quality_columns: bool
    available_remote_data: bool
    update_remote_data: bool
    import_to_influxdb: bool
    verbosity: int


class _HistDataCom:  # noqa:R701
    """Pull market data from histdata.com and import it into influxDB."""

    def __init__(self, options: Options) -> None:  # noqa:CCR001
        # pylint: disable=import-outside-toplevel
        """Initialize _HistDataCom Class.

        Args:
            options (Options): from histdata.options import Options

        Set User () or Default Arguments respectively utilizing the
        self.ArgParser and self.Options classes.
          - ArgParser()():
              - ()(): use an IIFE to allow argparse to get garbage collected
              - ()(): ArgParser.__call__ returns updated Options object
              - Options.to_dict(): get the declared option values
              - ArgParser._arg_list_to_set(...)
                  - Normalize iterable user arguments whose values are lists and
                    make them sets instead
          - .copy(): decouple for GC using a hard copy of user args
        """
        self.options = ArgParser(options)()
        configure_logging(self.options.verbosity)
        self.context = _resolve_runtime_context(self.options)
        self.options.api_return_type = self.context.api_return_type

    def run(  # noqa:CCR001,CFQ004,CCR001,R701
        self,
    ) -> list | dict | PolarsDataFrame | DataFrame | Table | None:
        """Execute. histdatacom's execution order.

        Returns:
            list | dict | PolarsDataFrame | DataFrame | Table | None:

            Data (PolarsDataFrame | DataFrame | Table):
                    a Polars DataFrame, pandas DataFrame, or pyarrow Table.
            List of dicts:  [
                                {
                                    "timeframe": timeframe,
                                    "pair": pair,
                                    "records": [record, record, ...],
                                    "data": PolarsDataFrame | DataFrame | Table,
                                },
                                ...
                                ...
                            ]


        """
        if self.context.version:
            if not self.context.from_api:
                print(histdatacom.__version__)  # noqa:T201
            return histdatacom.__version__

        if self.context.quality_preflight:
            return self._run_quality_preflight()

        return self._run_orchestration_job()

    def _run_quality_preflight(self) -> dict[str, Any]:
        """Run local cache-scale quality preflight without Temporal submit."""
        target_root = (
            self.context.quality_paths[0]
            if self.context.quality_paths
            else self.context.args["data_directory"]
        )
        pair_groups = _tuple_from_sequence_payload(
            self.context.request.metadata.get("pair_groups")
        )
        payload: dict[str, Any] = dict(
            run_cache_quality_preflight(
                target_root,
                pairs=self.context.request.pairs,
                pair_groups=pair_groups,
                formats=self.context.request.formats,
                timeframes=self.context.request.timeframes,
                quality_check_groups=self.context.quality_check_groups,
                quality_profile=self.context.quality_profile,
                sample_size=self.context.quality_preflight_sample_size,
            )
        )
        if self.context.quality_preflight_report_path:
            report_path = Path(
                self.context.quality_preflight_report_path
            ).expanduser()
            payload["report_path"] = str(
                publish_safe_path(str(report_path.resolve(strict=False)))
            )
            write_quality_preflight_report(payload, report_path)
        if self.context.from_api:
            return payload
        print(format_quality_preflight_console_summary(payload))  # noqa:T201
        if payload.get("status") == "fail":
            raise SystemExit(1)
        return payload

    def _run_orchestration_job(
        self,
    ) -> list | dict | PolarsDataFrame | DataFrame | Table:
        """Submit this run to the Temporal orchestration client boundary."""
        self._warn_before_large_quality_run_without_preflight()
        try:
            result = self._submit_orchestration_job()
        except OrchestrationUnavailableError as err:
            if self.context.from_api:
                raise
            print(  # noqa:T201
                format_exception_for_cli(
                    err,
                    title="HistData orchestration unavailable",
                ),
                file=sys.stderr,
            )
            raise SystemExit(1) from err

        payload = result.to_dict()
        if (
            self.context.data_quality or self.context.repo_quality_refresh
        ) and self.context.orchestration_wait_result:
            quality_payload = _quality_payload_from_orchestration_payload(
                payload
            )
            if quality_payload is not None:
                if self.context.from_api:
                    return quality_payload
                print(  # noqa:T201
                    _format_orchestration_quality_console_summary(
                        quality_payload
                    )
                )
                quality_exit_code = _quality_orchestration_exit_code(
                    quality_payload
                )
                if quality_exit_code:
                    raise SystemExit(quality_exit_code)
                if _orchestration_payload_failed(payload):
                    _print_orchestration_payload_failure(payload)
                    raise SystemExit(1)
                return payload

        if self._should_materialize_orchestration_repository_return():
            if _orchestration_repository_payload_failed(payload):
                if self.context.from_api:
                    return (
                        _repository_available_data_from_orchestration_payload(
                            payload
                        )
                        or {}
                    )
                print_repository_failure(
                    _repository_failure_code_from_orchestration_payload(payload)
                )
                raise SystemExit(1)

            available_data = (
                _repository_available_data_from_orchestration_payload(payload)
            )
            if available_data is not None:
                if self.context.from_api:
                    return available_data
                print_repository_table(
                    available_data,
                    include_quality=self.context.repo_quality_columns,
                )
                raise SystemExit(0)

        if (
            self.context.orchestration_wait_result
            and _orchestration_payload_failed(payload)
        ):
            if not self.context.from_api:
                print(
                    json.dumps(payload, indent=2, sort_keys=True)
                )  # noqa:T201
                _print_orchestration_payload_failure(payload)
                raise SystemExit(1)
            return payload

        if self._should_materialize_orchestration_api_return(payload):
            records = _cache_records_from_orchestration_payload(payload)
            if records:
                return self._materialize_orchestration_api_return(records)
        if not self.context.from_api:
            print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return payload

    def _warn_before_large_quality_run_without_preflight(self) -> None:
        """Warn before a large cache-backed quality run without evidence."""
        if not self.context.data_quality or self.context.from_api:
            return
        pair_groups = _tuple_from_sequence_payload(
            self.context.request.metadata.get("pair_groups")
        )
        warning = quality_run_preflight_warning(
            self.context.quality_paths,
            pairs=self.context.request.pairs,
            pair_groups=pair_groups,
            formats=self.context.request.formats,
            timeframes=self.context.request.timeframes,
            quality_check_groups=self.context.quality_check_groups,
            evidence_path=self.context.quality_preflight_evidence_path,
            evidence_max_age_seconds=(
                self.context.quality_preflight_evidence_max_age_seconds
            ),
            allow_stale_evidence=(
                self.context.quality_preflight_evidence_allow_stale
            ),
        )
        if warning is None:
            return
        print(  # noqa:T201
            format_quality_run_preflight_warning(warning),
            file=sys.stderr,
        )

    def _submit_orchestration_job(self) -> JobResult:
        """Submit an orchestration job with foreground progress when useful."""
        kwargs = {
            "start_if_needed": self.context.orchestration_start,
            "wait_for_result": self.context.orchestration_wait_result,
        }
        if self.context.orchestration_keep_runtime:
            kwargs["keep_runtime"] = True
        if (
            self.context.from_api
            or not self.context.orchestration_wait_result
            or not sys.stdout.isatty()
        ):
            return submit_run_request_and_observe_sync(
                self.context.request,
                **kwargs,
            )
        with LiveJobProgressRenderer(
            health_provider=operational_health_provider_for_request(
                self.context.request
            )
        ) as progress_renderer:
            return submit_run_request_and_observe_sync(
                self.context.request,
                progress_observer=progress_renderer.update,
                **kwargs,
            )

    def _should_materialize_orchestration_repository_return(self) -> bool:
        """Return whether a waited orchestration repo request should mimic legacy IO."""
        return bool(
            self.context.orchestration_wait_result
            and (
                self.context.available_remote_data
                or self.context.update_remote_data
            )
        )

    def _should_materialize_orchestration_api_return(
        self, payload: dict
    ) -> bool:
        """Return whether a completed orchestration run should mimic API returns."""
        return bool(
            self.context.from_api
            and self.context.api_return_type
            and self.context.orchestration_wait_result
            and payload.get("status") == "completed"
            and payload.get("result")
        )

    def _materialize_orchestration_api_return(
        self,
        records: list[Record],
    ) -> list | PolarsDataFrame | DataFrame | Table:
        """Rebuild the legacy API dataframe return from cache artifacts."""
        from histdatacom.api import Api

        return Api().merge_records(
            records,
            return_type=str(self.context.api_return_type or ""),
        )


def _resolve_runtime_context(options: Options) -> RuntimeContext:
    """Resolve launch values without touching process-global config."""
    args = ArgParser.arg_list_to_set(options.to_dict()).copy()
    expanded_pairs = set(
        expand_pair_selection(
            args.get("pairs") or (),
            args.get("pair_groups") or (),
        )
    )
    args["pairs"] = expanded_pairs
    options.pairs = expanded_pairs
    args["default_download_dir"] = set_working_data_dir(args["data_directory"])
    args["api_return_type"] = normalize_api_return_type(args["api_return_type"])
    options.api_return_type = args["api_return_type"]
    _attach_influx_config_metadata(options, args)
    try:
        should_submit_to_orchestration(args)
    except ValueError as err:
        raise ValueError(FOREGROUND_RUNTIME_REMOVED_MESSAGE) from err
    request = RunRequest.from_options(options)
    frozen_args = MappingProxyType(
        {key: _freeze_runtime_arg(value) for key, value in args.items()}
    )
    return RuntimeContext(
        args=frozen_args,
        request=request,
        version=bool(args["version"]),
        from_api=bool(args["from_api"]),
        orchestration_start=bool(args["orchestration_start"]),
        orchestration_keep_runtime=bool(args["orchestration_keep_runtime"]),
        orchestration_wait_result=bool(args["orchestration_wait_result"]),
        api_return_type=args["api_return_type"],
        data_quality=bool(args["data_quality"]),
        quality_paths=tuple(
            str(path) for path in (args.get("quality_paths") or ())
        ),
        quality_check_groups=tuple(
            sorted(
                str(group) for group in (args.get("quality_check_groups") or ())
            )
        ),
        quality_report_path=(
            None
            if args.get("quality_report_path") is None
            else str(args["quality_report_path"])
        ),
        quality_fail_on=str(args["quality_fail_on"]),
        quality_max_errors=int(args["quality_max_errors"]),
        quality_max_warnings=int(args["quality_max_warnings"]),
        quality_preflight=bool(args["quality_preflight"]),
        quality_preflight_evidence_allow_stale=bool(
            args["quality_preflight_evidence_allow_stale"]
        ),
        quality_preflight_evidence_max_age_seconds=int(
            args["quality_preflight_evidence_max_age_seconds"]
        ),
        quality_preflight_evidence_path=(
            None
            if args.get("quality_preflight_evidence_path") is None
            else str(args["quality_preflight_evidence_path"])
        ),
        quality_preflight_report_path=(
            None
            if args.get("quality_preflight_report_path") is None
            else str(args["quality_preflight_report_path"])
        ),
        quality_preflight_sample_size=int(
            args["quality_preflight_sample_size"]
        ),
        quality_profile_path=str(args.get("quality_profile_path") or ""),
        quality_profile=dict(args.get("quality_profile") or {}),
        repo_quality_refresh=bool(args["repo_quality_refresh"]),
        repo_quality_columns=bool(args["repo_quality_columns"]),
        available_remote_data=bool(args["available_remote_data"]),
        update_remote_data=bool(args["update_remote_data"]),
        import_to_influxdb=bool(args["import_to_influxdb"]),
        verbosity=int(args["verbosity"]),
    )


def _attach_influx_config_metadata(
    options: Options,
    args: dict[str, Any],
) -> None:
    """Snapshot caller-local Influx config before orchestration handoff."""
    if not bool(args.get("import_to_influxdb")):
        return
    metadata = dict(getattr(options, "metadata", {}) or {})
    if isinstance(metadata.get("influx_config"), Mapping):
        _validate_influx_metadata_config(metadata["influx_config"])
        options.metadata = metadata
        args["metadata"] = metadata
        return
    influx_yaml = load_influx_yaml()
    influx_config = dict(influx_yaml.get("influxdb") or {})
    missing = [
        key
        for key in ("org", "bucket", "url", "token")
        if not influx_config.get(key)
    ]
    if missing:
        missing_text = ", ".join(missing)
        raise InfluxConfigurationError(
            "influxdb.yaml is missing required influxdb keys: "
            f"{missing_text}."
        )
    metadata["influx_config"] = {
        "INFLUX_ORG": str(influx_config.get("org", "") or ""),
        "INFLUX_BUCKET": str(influx_config.get("bucket", "") or ""),
        "INFLUX_URL": str(influx_config.get("url", "") or ""),
        "INFLUX_TOKEN": str(influx_config.get("token", "") or ""),
    }
    options.metadata = metadata
    args["metadata"] = metadata


def _validate_influx_metadata_config(config: Mapping[str, Any]) -> None:
    """Validate serialized orchestration Influx connection keys."""
    missing = [
        key
        for key in ("INFLUX_ORG", "INFLUX_BUCKET", "INFLUX_URL", "INFLUX_TOKEN")
        if not config.get(key)
    ]
    if missing:
        missing_text = ", ".join(missing)
        raise InfluxConfigurationError(
            "influx metadata is missing required keys: " f"{missing_text}."
        )


def _freeze_runtime_arg(value: Any) -> Any:
    """Return an immutable equivalent for container-like runtime args."""
    if isinstance(value, set):
        return frozenset(value)
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_runtime_arg(item) for key, item in value.items()}
        )
    return value


def main(
    options: Options | None = None,
) -> list | dict | PolarsDataFrame | DataFrame | Table | int | None:
    """Execute. Entry-point for histdatacom.

    Args:
        options (Options): a histdatacom.options Options object.

    Returns:
        list | dict | PolarsDataFrame | DataFrame | Table | None:

            Data (PolarsDataFrame | DataFrame | Table):
                    a Polars DataFrame, pandas DataFrame, or pyarrow Table.
            List of dicts:  [
                                {
                                    "timeframe": timeframe,
                                    "pair": pair,
                                    "records": [record, record, ...],
                                    "data": PolarsDataFrame | DataFrame | Table,
                                },
                                ...
                                ...
                            ]
    """
    cli_args = sys.argv[1:] if not options else []
    routed_command = routed_command_from_cli_args(
        cli_args,
        {"analytics", "cleanup", "jobs", "quality", "runtime"},
    )
    if not options and routed_command == "cleanup":
        from histdatacom.cleanup_cli import main as cleanup_main

        return cleanup_main(
            remove_routed_command_from_cli_args(cli_args, "cleanup")
        )
    if not options and routed_command == "jobs":
        from histdatacom.orchestration.cli import jobs_main

        return jobs_main(remove_routed_command_from_cli_args(cli_args, "jobs"))
    if not options and routed_command == "quality":
        from histdatacom.quality_cli import main as quality_main

        return quality_main(
            remove_routed_command_from_cli_args(cli_args, "quality")
        )
    if not options and routed_command == "runtime":
        from histdatacom.orchestration.cli import main as runtime_main

        return runtime_main(
            remove_routed_command_from_cli_args(cli_args, "runtime")
        )
    if not options and routed_command == "analytics":
        from histdatacom.data_analytics.cli import main as analytics_main

        return analytics_main(
            remove_routed_command_from_cli_args(cli_args, "analytics")
        )

    if not options:
        options = Options()
        _HistDataCom(options).run()
        return None
    options.from_api = True
    return _HistDataCom(options).run()


def _cache_records_from_orchestration_payload(payload: dict) -> list[Record]:
    """Return legacy records reconstructed from orchestration cache artifacts."""
    records: list[Record] = []
    seen_paths: set[str] = set()
    for artifact in _iter_artifact_payloads(payload):
        if artifact.get("kind") != "cache":
            continue
        path = Path(str(artifact.get("path", "")))
        if path.name != CACHE_FILENAME or not path.is_file():
            continue
        resolved_path = str(path.resolve())
        if resolved_path in seen_paths:
            continue
        seen_paths.add(resolved_path)
        records.append(_record_from_cache_artifact(path, artifact))
    return records


def _repository_available_data_from_orchestration_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return legacy repository data from orchestration result metrics."""
    for item in _iter_mapping_payloads(payload):
        metrics = item.get("metrics")
        if not isinstance(metrics, Mapping) or "available_data" not in metrics:
            continue
        available_data = metrics.get("available_data")
        if isinstance(available_data, Mapping):
            return {
                str(pair): dict(value) if isinstance(value, Mapping) else value
                for pair, value in available_data.items()
            }
    return None


def _orchestration_repository_payload_failed(
    payload: Mapping[str, Any],
) -> bool:
    """Return whether the waited orchestration result represents repo failure."""
    result = payload.get("result")
    if isinstance(result, Mapping):
        status = str(result.get("status", "") or "").lower()
        if status in {"failed", "cancelled"}:
            return True
    return bool(_repository_failure_code_from_orchestration_payload(payload))


def _orchestration_payload_failed(payload: Mapping[str, Any]) -> bool:
    """Return whether a waited orchestration result represents failed work."""
    return _orchestration_failure_status(payload) in {
        WorkStatus.FAILED,
        WorkStatus.CANCELLED,
    }


def _orchestration_failure_status(
    payload: Mapping[str, Any],
) -> WorkStatus | None:
    """Return the terminal failure status from an orchestration payload."""
    candidates: list[Any] = [payload.get("status")]
    result = payload.get("result")
    if isinstance(result, Mapping):
        candidates.append(result.get("status"))
    snapshot = payload.get("snapshot")
    if isinstance(snapshot, Mapping):
        candidates.append(snapshot.get("status"))
        candidates.append(snapshot.get("lifecycle"))

    for candidate in candidates:
        status = WorkStatus.from_value(candidate)
        if status in {WorkStatus.FAILED, WorkStatus.CANCELLED}:
            return status
    return None


def _print_orchestration_payload_failure(payload: Mapping[str, Any]) -> None:
    """Print a concise CLI error for failed waited orchestration jobs."""
    status = _orchestration_failure_status(payload) or WorkStatus.FAILED
    failure = _orchestration_failure_info(payload)
    if failure is not None:
        print(  # noqa:T201
            format_failure_info_for_cli(
                _failure_info_with_orchestration_status(failure, status),
                title=f"HistData orchestration job {status.value.lower()}",
            ),
            file=sys.stderr,
        )
        return
    message = _orchestration_failure_message(payload)
    suffix = f": {message}" if message else ""
    print(
        f"error: orchestration job {status.value.lower()}{suffix}",
        file=sys.stderr,
    )  # noqa:T201


def _orchestration_failure_message(payload: Mapping[str, Any]) -> str:
    """Return the first useful failure message from an orchestration payload."""
    failure = _orchestration_failure_info(payload)
    if failure is not None and failure.message:
        return str(failure.message)
    for item in _iter_mapping_payloads(payload):
        last_error = item.get("last_error")
        if last_error:
            return str(last_error)
    return ""


def _orchestration_failure_info(
    payload: Mapping[str, Any],
) -> FailureInfo | None:
    """Return the first structured failure in an orchestration payload."""
    for item in _iter_mapping_payloads(payload):
        failure = item.get("failure")
        if isinstance(failure, Mapping):
            return FailureInfo.from_dict(failure)
    return None


def _failure_info_with_orchestration_status(
    failure: FailureInfo,
    status: WorkStatus,
) -> FailureInfo:
    """Attach workflow terminal status without mutating the source payload."""
    detail = dict(failure.detail)
    detail.setdefault("orchestration_status", status.value)
    return FailureInfo(
        code=failure.code,
        message=failure.message,
        retryable=failure.retryable,
        detail=detail,
    )


def _repository_failure_code_from_orchestration_payload(
    payload: Mapping[str, Any],
) -> str:
    """Return the first structured failure code in an orchestration payload."""
    for item in _iter_mapping_payloads(payload):
        failure = item.get("failure")
        if not isinstance(failure, Mapping):
            continue
        code = failure.get("code")
        if code:
            return str(code)
    return ""


def _quality_payload_from_orchestration_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return the bounded quality payload from an orchestration result."""
    for item in _iter_mapping_payloads(payload):
        quality = item.get("quality")
        if _is_data_quality_payload(quality):
            return dict(quality)
        metrics = item.get("metrics")
        if isinstance(metrics, Mapping):
            quality = metrics.get("quality")
            if _is_data_quality_payload(quality):
                return dict(quality)
    return None


def _is_data_quality_payload(value: object) -> TypeGuard[Mapping[str, Any]]:
    return isinstance(value, Mapping) and value.get("operation") == (
        "data-quality"
    )


def _format_orchestration_quality_console_summary(
    quality_payload: Mapping[str, Any],
) -> str:
    """Return a compact CLI summary from orchestration quality metadata."""
    summary = _mapping_from_payload(quality_payload.get("summary"))
    check_groups = quality_payload.get("check_groups")
    checks = (
        ", ".join(str(group) for group in check_groups)
        if isinstance(check_groups, list) and check_groups
        else "all"
    )
    lines = [
        "Data quality assessment",
        f"checks: {checks}",
    ]
    if "error" in quality_payload:
        lines.extend(
            (
                "status: failed",
                f"error: {quality_payload['error']}",
            )
        )
        return "\n".join(lines)

    lines.extend(
        (
            f"status: {summary.get('status', 'unknown')}",
            _format_quality_target_counts(quality_payload, summary),
            (
                "findings: "
                f"{summary.get('finding_count', 0)} "
                f"info: {summary.get('info_count', 0)} "
                f"warning: {summary.get('warning_count', 0)} "
                f"error: {summary.get('error_count', 0)}"
            ),
        )
    )
    artifact = _mapping_from_payload(quality_payload.get("report_artifact"))
    if artifact.get("path"):
        lines.append(f"report: {artifact['path']}")
    report_disposition = _mapping_from_payload(
        quality_payload.get("quality_report")
    )
    if report_disposition.get("deleted"):
        lines.append("quality report: scratch report deleted after validation")
    elif report_disposition.get("delete_error"):
        lines.append(
            "quality report cleanup: " f"{report_disposition['delete_error']}"
        )
    source_cleanliness = _mapping_from_payload(
        quality_payload.get("source_cleanliness")
    )
    if source_cleanliness:
        source_count = int(
            source_cleanliness.get("source_artifact_count", 0) or 0
        )
        source_state = source_cleanliness.get("state", "unknown")
        lines.append(
            "source artifacts: "
            f"{source_state} ({source_count} transient ZIP/CSV/XLS/XLSX)"
        )
    repo_quality = _mapping_from_payload(quality_payload.get("repo_quality"))
    repo_artifact = _mapping_from_payload(repo_quality.get("repo_artifact"))
    if repo_quality.get("refreshed") and repo_artifact.get("path"):
        lines.append(f"repo quality: {repo_artifact['path']}")
    decision = _mapping_from_payload(quality_payload.get("exit_decision"))
    if decision.get("reason"):
        lines.append(f"decision: {decision['reason']}")
    if int(summary.get("target_count", 0) or 0) == 0:
        lines.append("No data quality targets discovered.")
    lines.extend(_format_quality_target_sections(quality_payload))
    return "\n".join(lines)


def _quality_orchestration_exit_code(quality_payload: Mapping[str, Any]) -> int:
    decision = _mapping_from_payload(quality_payload.get("exit_decision"))
    try:
        return int(decision.get("exit_code", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _mapping_from_payload(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _tuple_from_sequence_payload(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple, set, frozenset)):
        return ()
    return tuple(str(item) for item in value)


def _format_quality_target_counts(
    quality_payload: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    status_counts = _mapping_from_payload(
        quality_payload.get("target_status_counts")
    )
    if status_counts:
        return (
            "targets: "
            f"{summary.get('target_count', 0)} "
            f"clean: {status_counts.get('clean', 0)} "
            f"warning: {status_counts.get('warning', 0)} "
            f"failed: {status_counts.get('failed', 0)}"
        )
    target_summaries = _quality_target_summaries(quality_payload)
    if not target_summaries:
        return "targets: " f"{summary.get('target_count', 0)}"
    return (
        "targets: "
        f"{summary.get('target_count', 0)} "
        f"clean: {_quality_target_count(target_summaries, 'clean')} "
        f"warning: {_quality_target_count(target_summaries, 'warning')} "
        f"failed: {_quality_target_count(target_summaries, 'failed')}"
    )


def _format_quality_target_sections(
    quality_payload: Mapping[str, Any],
) -> list[str]:
    target_summaries = _quality_target_summaries(quality_payload)
    if not target_summaries:
        return []
    lines: list[str] = []
    for status, title in (
        ("clean", "Clean files"),
        ("warning", "Warning files"),
        ("failed", "Failed files"),
    ):
        lines.extend(("", title))
        target_lines = [
            _format_quality_target_summary(item)
            for item in target_summaries
            if str(item.get("status", "") or "") == status
        ]
        lines.extend(target_lines or ["- none"])
    return lines


def _quality_target_summaries(
    quality_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    raw_summaries = quality_payload.get("target_summaries")
    if not isinstance(raw_summaries, list):
        return []
    return [dict(item) for item in raw_summaries if isinstance(item, Mapping)]


def _quality_target_count(
    target_summaries: list[dict[str, Any]],
    status: str,
) -> int:
    return sum(
        1
        for item in target_summaries
        if str(item.get("status", "") or "") == status
    )


def _format_quality_target_summary(summary: Mapping[str, Any]) -> str:
    target = _mapping_from_payload(summary.get("target"))
    return (
        f"- {target.get('kind', 'unknown')}: {target.get('path', '')} "
        f"(findings={summary.get('finding_count', 0)}, "
        f"warnings={summary.get('warning_count', 0)}, "
        f"errors={summary.get('error_count', 0)})"
    )


def _record_from_cache_artifact(
    path: Path,
    artifact: dict,
) -> Record:
    metadata = dict(artifact.get("metadata") or {})
    return Record(
        status=WorkStatus.CACHE_READY,
        data_dir=f"{path.parent}{os.sep}",
        cache_filename=path.name,
        cache_line_count=str(metadata.get("line_count", "") or ""),
        cache_start=str(metadata.get("start", "") or ""),
        cache_end=str(metadata.get("end", "") or ""),
        data_timeframe=str(metadata.get("timeframe", "") or ""),
        data_fxpair=str(metadata.get("pair", "") or ""),
        data_format="ascii",
    )


def _iter_mapping_payloads(value: object) -> list[Mapping[str, Any]]:
    """Collect dictionaries from nested orchestration result payloads."""
    payloads: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        payloads.append(value)
        for item in value.values():
            payloads.extend(_iter_mapping_payloads(item))
    elif isinstance(value, list):
        for item in value:
            payloads.extend(_iter_mapping_payloads(item))
    return payloads


def _iter_artifact_payloads(value: object) -> list[dict]:
    """Collect artifact dictionaries from nested orchestration result payloads."""
    artifacts: list[dict] = []
    if isinstance(value, dict):
        if "kind" in value and "path" in value:
            artifacts.append(value)
        for item in value.values():
            artifacts.extend(_iter_artifact_payloads(item))
    elif isinstance(value, list):
        for item in value:
            artifacts.extend(_iter_artifact_payloads(item))
    return artifacts


if __name__ == "__main__":
    raise SystemExit(main())

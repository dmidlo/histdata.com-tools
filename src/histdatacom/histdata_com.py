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
import subprocess
import sys
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping, Sequence, TypeGuard, cast

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
    QUALITY_PREFLIGHT_VALIDATION_EVIDENCE_SCHEMA_VERSION,
    format_quality_preflight_console_summary,
    format_quality_run_preflight_warning,
    quality_preflight_to_markdown,
    quality_preflight_validation_evidence_payload,
    quality_preflight_validation_evidence_to_json,
    quality_run_preflight_warning,
    register_quality_preflight_evidence_artifact,
    run_cache_quality_preflight,
    write_quality_preflight_evidence_artifact,
    write_quality_preflight_markdown_report,
    write_quality_preflight_report,
)
from histdatacom.data_quality.profiles import (
    QualityProfile,
    quality_profile_from_value,
    quality_profile_resolution_from_value,
    quality_profile_source_kind,
)
from histdatacom.data_quality.reporting import (
    format_cross_series_fingerprint_lines,
    format_fingerprint_distribution_attention_lines,
    format_fingerprint_distribution_summary_lines,
    format_fingerprint_readiness_risk_lines,
    format_fingerprint_topology_attention_lines,
    format_fingerprint_topology_summary_lines,
    format_quality_engine_skip_lines,
    format_quality_next_action_lines,
    format_quality_remediation_coverage_lines,
)
from histdatacom.fx_enums import expand_pair_selection
from histdatacom.repository_output import (
    print_repository_failure,
    print_repository_table,
)
from histdatacom.histdata_ascii import CACHE_FILENAME
from histdatacom.publication_safety import publish_safe_path
from histdatacom.publication_safety import publish_safe_json_mapping
from histdatacom.records import Record
from histdatacom.runtime_contracts import (
    FailureInfo,
    JSONValue,
    RunRequest,
    WorkStatus,
)
from histdatacom.scheduled_run_bundle import build_scheduled_run_bundle
from histdatacom.orchestration.client import (
    JobResult,
    OrchestrationOverlapError,
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

WINDOWS_RUNTIME_REEXEC_ENV = "HISTDATACOM_WINDOWS_RUNTIME_REEXEC"
WINDOWS_PYTHON_EXECUTABLE_NAMES = frozenset({"python.exe", "pythonw.exe"})
WINDOWS_HISTDATACOM_LAUNCHER_NAMES = frozenset({"histdatacom.exe"})
QUALITY_PROFILE_PREVIEW_SCHEMA_VERSION = (
    "histdatacom.quality-profile-preview.v1"
)
QUALITY_PROFILE_PREVIEW_EXPLANATION_SCHEMA_VERSION = (
    "histdatacom.quality-profile-preview-explanation.v1"
)
QUALITY_PROFILE_PREVIEW_DIFF_SCHEMA_VERSION = (
    "histdatacom.quality-profile-preview-diff.v1"
)
QUALITY_PROFILE_PREVIEW_FORMATS = frozenset({"json", "text", "markdown"})
QUALITY_PROFILE_PREVIEW_SOURCE_LIMIT = 128
QUALITY_PROFILE_PREVIEW_DIFF_LIMIT = 128
QUALITY_PROFILE_PREVIEW_DISPLAY_VALUE_LIMIT = 120


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
    quality_preflight_markdown: bool
    quality_preflight_markdown_report_path: str | None
    quality_preflight_profile_preview_format: str
    quality_preflight_profile_preview_output_path: str
    quality_preflight_report_path: str | None
    quality_preflight_run_validation: bool
    quality_preflight_sample_size: int
    quality_preflight_validation_report_path: str | None
    quality_preflight_validation_evidence_path: str
    quality_profile_path: str
    quality_profile: Mapping[str, Any]
    quality_profile_resolution: Mapping[str, Any]
    quality_profile_preview: bool
    quality_profile_preview_format: str
    quality_profile_preview_output_path: str
    repo_quality_refresh: bool
    repo_quality_columns: bool
    available_remote_data: bool
    update_remote_data: bool
    import_to_influxdb: bool
    verbosity: int
    request_bundle_out: str
    request_json_out: str


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

        if self.context.quality_profile_preview:
            payload = _quality_profile_preview_payload(self.context)
            rendered = _format_quality_profile_preview(
                payload,
                output_format=self.context.quality_profile_preview_format,
            )
            if self.context.quality_profile_preview_output_path:
                _write_text_payload(
                    rendered,
                    self.context.quality_profile_preview_output_path,
                )
            elif not self.context.from_api:
                print(rendered)  # noqa:T201
            return payload

        if self.context.request_json_out or self.context.request_bundle_out:
            return self._export_run_artifacts()

        if self.context.quality_preflight:
            return self._run_quality_preflight()

        return self._run_orchestration_job()

    def _export_run_artifacts(self) -> dict[str, JSONValue]:
        """Write requested non-submitting run artifacts."""
        if (
            self.context.request_json_out == "-"
            and self.context.request_bundle_out == "-"
        ):
            raise ValueError(
                "request JSON and request bundle exports cannot both target stdout"
            )
        request_payload: dict[str, JSONValue] = self.context.request.to_dict()
        if self.context.request_json_out:
            _write_run_request_json(
                request_payload, self.context.request_json_out
            )
        if not self.context.request_bundle_out:
            return request_payload
        bundle_payload: dict[str, JSONValue] = build_scheduled_run_bundle(
            self.context.request
        ).to_dict()
        _write_json_payload(bundle_payload, self.context.request_bundle_out)
        return bundle_payload

    def _export_run_request_json(self) -> dict[str, JSONValue]:
        """Write the resolved RunRequest payload without submitting work."""
        payload: dict[str, JSONValue] = self.context.request.to_dict()
        _write_run_request_json(payload, self.context.request_json_out)
        return payload

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
                validation_report_path=(
                    self.context.quality_preflight_validation_report_path
                ),
                run_validation=self.context.quality_preflight_run_validation,
            )
        )
        _attach_quality_preflight_profile_preview(payload, self.context)
        _attach_quality_preflight_validation_evidence(payload, self.context)
        if self.context.quality_preflight_report_path:
            report_path = Path(
                self.context.quality_preflight_report_path
            ).expanduser()
            payload["report_path"] = str(
                publish_safe_path(str(report_path.resolve(strict=False)))
            )
            write_quality_preflight_report(payload, report_path)
        if self.context.quality_preflight_markdown_report_path:
            markdown_path = Path(
                self.context.quality_preflight_markdown_report_path
            ).expanduser()
            payload["markdown_report_path"] = str(
                publish_safe_path(str(markdown_path.resolve(strict=False)))
            )
            write_quality_preflight_markdown_report(payload, markdown_path)
        if self.context.from_api:
            return payload
        if self.context.quality_preflight_markdown:
            print(quality_preflight_to_markdown(payload))  # noqa:T201
        else:
            print(
                format_quality_preflight_console_summary(payload)
            )  # noqa:T201
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
        except OrchestrationOverlapError as err:
            if self.context.from_api:
                raise
            print(  # noqa:T201
                format_exception_for_cli(
                    err,
                    title="HistData scheduled job overlap blocked",
                ),
                file=sys.stderr,
            )
            raise SystemExit(err.exit_code or 1) from err
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
        quality_preflight_markdown=bool(args["quality_preflight_markdown"]),
        quality_preflight_markdown_report_path=(
            None
            if args.get("quality_preflight_markdown_report_path") is None
            else str(args["quality_preflight_markdown_report_path"])
        ),
        quality_preflight_profile_preview_format=str(
            args.get("quality_preflight_profile_preview_format") or "json"
        ),
        quality_preflight_profile_preview_output_path=str(
            args.get("quality_preflight_profile_preview_output_path") or ""
        ),
        quality_preflight_report_path=(
            None
            if args.get("quality_preflight_report_path") is None
            else str(args["quality_preflight_report_path"])
        ),
        quality_preflight_run_validation=bool(
            args["quality_preflight_run_validation"]
        ),
        quality_preflight_sample_size=int(
            args["quality_preflight_sample_size"]
        ),
        quality_preflight_validation_report_path=(
            None
            if args.get("quality_preflight_validation_report_path") is None
            else str(args["quality_preflight_validation_report_path"])
        ),
        quality_preflight_validation_evidence_path=str(
            args.get("quality_preflight_validation_evidence_path") or ""
        ),
        quality_profile_path=str(args.get("quality_profile_path") or ""),
        quality_profile=dict(args.get("quality_profile") or {}),
        quality_profile_resolution=dict(
            args.get("quality_profile_resolution") or {}
        ),
        quality_profile_preview=bool(args["quality_profile_preview"]),
        quality_profile_preview_format=str(
            args.get("quality_profile_preview_format") or "json"
        ),
        quality_profile_preview_output_path=str(
            args.get("quality_profile_preview_output_path") or ""
        ),
        repo_quality_refresh=bool(args["repo_quality_refresh"]),
        repo_quality_columns=bool(args["repo_quality_columns"]),
        available_remote_data=bool(args["available_remote_data"]),
        update_remote_data=bool(args["update_remote_data"]),
        import_to_influxdb=bool(args["import_to_influxdb"]),
        verbosity=int(args["verbosity"]),
        request_bundle_out=str(args.get("request_bundle_out") or ""),
        request_json_out=str(args.get("request_json_out") or ""),
    )


def _write_run_request_json(
    payload: Mapping[str, Any],
    destination: str,
) -> None:
    """Write a deterministic JSON RunRequest payload to stdout or a file."""
    _write_json_payload(payload, destination)


def _write_json_payload(
    payload: Mapping[str, Any],
    destination: str,
) -> None:
    """Write a deterministic JSON payload to stdout or a file."""
    content = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if destination == "-":
        print(content, end="")  # noqa:T201
        return
    path = Path(destination).expanduser()
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_text_payload(content: str, destination: str) -> None:
    """Write deterministic text content to stdout or a file."""
    rendered = content if content.endswith("\n") else f"{content}\n"
    if destination == "-":
        print(rendered, end="")  # noqa:T201
        return
    path = Path(destination).expanduser()
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")


def _attach_quality_preflight_profile_preview(
    payload: dict[str, Any],
    context: RuntimeContext,
) -> None:
    """Write an optional profile preview artifact into preflight evidence."""
    destination = context.quality_preflight_profile_preview_output_path.strip()
    if not destination:
        return
    output_format = context.quality_preflight_profile_preview_format
    preview_payload = publish_safe_json_mapping(
        _quality_profile_preview_payload(
            context,
            preview_format=output_format,
            preview_output_path=destination,
        )
    )
    rendered = _format_quality_profile_preview(
        preview_payload,
        output_format=output_format,
    )
    artifact = write_quality_preflight_evidence_artifact(
        rendered,
        destination,
        kind="quality-profile-preview",
        output_format=output_format,
        schema_version=QUALITY_PROFILE_PREVIEW_SCHEMA_VERSION,
        label="Profile preview",
        console_label="profile preview",
    )
    register_quality_preflight_evidence_artifact(
        payload,
        "quality_profile_preview",
        artifact,
        legacy_key="quality_profile_preview",
    )


def _attach_quality_preflight_validation_evidence(
    payload: dict[str, Any],
    context: RuntimeContext,
) -> None:
    """Write optional bounded validation evidence into the artifact map."""
    destination = context.quality_preflight_validation_evidence_path.strip()
    if not destination:
        return
    validation_payload = quality_preflight_validation_evidence_payload(payload)
    artifact = write_quality_preflight_evidence_artifact(
        quality_preflight_validation_evidence_to_json(validation_payload),
        destination,
        kind="quality-preflight-validation-evidence",
        output_format="json",
        schema_version=(QUALITY_PREFLIGHT_VALIDATION_EVIDENCE_SCHEMA_VERSION),
        label="Validation evidence",
        console_label="validation evidence",
    )
    artifact["generated_at_utc"] = str(
        validation_payload.get("generated_at_utc", "")
    )
    artifact["validation_state"] = str(validation_payload.get("state", ""))
    register_quality_preflight_evidence_artifact(
        payload,
        "validation_evidence",
        artifact,
        legacy_key="validation_evidence",
    )


def _format_quality_profile_preview(
    payload: Mapping[str, JSONValue],
    *,
    output_format: str,
) -> str:
    """Return a deterministic quality-profile preview rendering."""
    normalized_format = output_format.strip().lower()
    if normalized_format not in QUALITY_PROFILE_PREVIEW_FORMATS:
        raise ValueError(
            "quality profile preview format must be one of "
            f"{sorted(QUALITY_PROFILE_PREVIEW_FORMATS)}"
        )
    if normalized_format == "json":
        return json.dumps(payload, indent=2, sort_keys=True)
    if normalized_format == "markdown":
        return _format_quality_profile_preview_markdown(payload)
    return _format_quality_profile_preview_text(payload)


def _format_quality_profile_preview_text(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return a bounded text rendering of a quality-profile preview."""
    explanation = _preview_mapping(payload.get("profile_explanation"))
    profile_inputs = _preview_mapping(payload.get("profile_inputs"))
    cli_overrides = _preview_mapping(payload.get("cli_overrides"))
    lines = [
        "Quality Profile Preview",
        f"modes: {_preview_quality_modes_text(payload)}",
        f"profile: {_preview_profile_source_text(payload)}",
        "",
        "Profile Inputs",
    ]
    for key, value in sorted(profile_inputs.items()):
        lines.append(f"- {key}: {_preview_display_value(value)}")
    lines.extend(["", "Input Channels"])
    for channel in _preview_mapping_rows(explanation.get("input_channels")):
        lines.append(f"- {_preview_channel_text(channel)}")
    lines.extend(["", "Explicit Overrides"])
    if cli_overrides:
        for key, value in sorted(cli_overrides.items()):
            lines.append(f"- {key}: {_preview_display_value(value)}")
    else:
        lines.append("- none")
    lines.extend(["", "Effective Diff From Built-In Defaults"])
    diff = _preview_mapping(explanation.get("effective_diff"))
    lines.append(
        f"- {_preview_bounded_summary_text(diff, row_label='changes')}"
    )
    for change in _preview_mapping_rows(diff.get("changes")):
        lines.append(f"- {_preview_change_text(change)}")
    lines.extend(["", "Value Sources"])
    value_sources = _preview_mapping(explanation.get("effective_value_sources"))
    lines.append(
        f"- {_preview_bounded_summary_text(value_sources, row_label='values')}"
    )
    for row in _preview_mapping_rows(value_sources.get("values")):
        lines.append(f"- {_preview_value_source_text(row)}")
    return "\n".join(lines)


def _format_quality_profile_preview_markdown(
    payload: Mapping[str, JSONValue],
) -> str:
    """Return a bounded Markdown rendering of a quality-profile preview."""
    explanation = _preview_mapping(payload.get("profile_explanation"))
    profile_inputs = _preview_mapping(payload.get("profile_inputs"))
    cli_overrides = _preview_mapping(payload.get("cli_overrides"))
    diff = _preview_mapping(explanation.get("effective_diff"))
    value_sources = _preview_mapping(explanation.get("effective_value_sources"))
    lines = [
        "# Quality Profile Preview",
        "",
        "| Field | Value |",
        "| --- | --- |",
        f"| Modes | {_markdown_cell(_preview_quality_modes_text(payload))} |",
        (
            f"| Profile | {_markdown_cell(_preview_profile_source_text(payload))} |"
        ),
        "",
        "## Profile Inputs",
        "",
        "| Input | Value |",
        "| --- | --- |",
    ]
    for key, value in sorted(profile_inputs.items()):
        lines.append(
            f"| {_markdown_cell(key)} | "
            f"{_markdown_cell(_preview_display_value(value))} |"
        )
    lines.extend(
        [
            "",
            "## Input Channels",
            "",
            "| Kind | Details |",
            "| --- | --- |",
        ]
    )
    for channel in _preview_mapping_rows(explanation.get("input_channels")):
        lines.append(
            f"| {_markdown_cell(str(channel.get('kind') or 'unknown'))} | "
            f"{_markdown_cell(_preview_channel_detail_text(channel))} |"
        )
    lines.extend(
        [
            "",
            "## Explicit Overrides",
            "",
            "| Path | Value |",
            "| --- | --- |",
        ]
    )
    if cli_overrides:
        for key, value in sorted(cli_overrides.items()):
            lines.append(
                f"| {_markdown_cell(key)} | "
                f"{_markdown_cell(_preview_display_value(value))} |"
            )
    else:
        lines.append("| none |  |")
    lines.extend(
        [
            "",
            "## Effective Diff From Built-In Defaults",
            "",
            f"{_preview_bounded_summary_text(diff, row_label='changes')}.",
            "",
            "| Path | Source | Before | After |",
            "| --- | --- | --- | --- |",
        ]
    )
    for change in _preview_mapping_rows(diff.get("changes")):
        lines.append(
            f"| {_markdown_cell(str(change.get('path') or ''))} | "
            f"{_markdown_cell(str(change.get('source') or 'unknown'))} | "
            f"{_markdown_cell(_preview_display_value(change.get('before')))} | "
            f"{_markdown_cell(_preview_display_value(change.get('after')))} |"
        )
    lines.extend(
        [
            "",
            "## Value Sources",
            "",
            f"{_preview_bounded_summary_text(value_sources, row_label='values')}.",
            "",
            "| Path | Source | Value |",
            "| --- | --- | --- |",
        ]
    )
    for row in _preview_mapping_rows(value_sources.get("values")):
        lines.append(
            f"| {_markdown_cell(str(row.get('path') or ''))} | "
            f"{_markdown_cell(str(row.get('source') or 'unknown'))} | "
            f"{_markdown_cell(_preview_display_value(row.get('value')))} |"
        )
    return "\n".join(lines)


def _preview_quality_modes_text(payload: Mapping[str, JSONValue]) -> str:
    """Return active quality modes for a preview payload."""
    modes = _preview_mapping(payload.get("quality_modes"))
    active_modes = [key for key, active in sorted(modes.items()) if active]
    return ", ".join(active_modes) if active_modes else "none"


def _preview_profile_source_text(payload: Mapping[str, JSONValue]) -> str:
    """Return a compact profile source summary."""
    explanation = _preview_mapping(payload.get("profile_explanation"))
    source = _preview_mapping(explanation.get("profile_source"))
    quality_profile = _preview_mapping(payload.get("quality_profile"))
    kind = str(source.get("kind") or quality_profile.get("source") or "")
    name = str(source.get("profile_name") or quality_profile.get("name") or "")
    source_path = str(
        source.get("source_path") or quality_profile.get("source_path") or ""
    )
    pieces = [kind or "unknown"]
    if name:
        pieces.append(f"name={name}")
    if source_path:
        pieces.append(f"path={source_path}")
    return " ".join(pieces)


def _preview_channel_text(channel: Mapping[str, JSONValue]) -> str:
    """Return one text row for an input channel."""
    kind = str(channel.get("kind") or "unknown")
    detail = _preview_channel_detail_text(channel)
    return f"{kind}: {detail}" if detail else kind


def _preview_channel_detail_text(channel: Mapping[str, JSONValue]) -> str:
    """Return compact detail text for an input channel."""
    parts: list[str] = []
    for key in (
        "description",
        "path",
        "profile_name",
        "profile_source",
        "source_path",
        "selected_by",
        "paths",
    ):
        value = channel.get(key)
        if value in (None, "", [], {}):
            continue
        parts.append(f"{key}={_preview_display_value(value)}")
    return "; ".join(parts)


def _preview_bounded_summary_text(
    summary: Mapping[str, JSONValue],
    *,
    row_label: str,
) -> str:
    """Return a compact bounded-row summary."""
    total = summary.get("changed_value_count", summary.get("value_count", 0))
    included = summary.get(
        "included_change_count", summary.get("included_value_count", 0)
    )
    omitted = summary.get(
        "omitted_change_count", summary.get("omitted_value_count", 0)
    )
    return f"{total} {row_label}, {included} shown, {omitted} omitted"


def _preview_change_text(change: Mapping[str, JSONValue]) -> str:
    """Return one text row for a default-profile diff change."""
    path = str(change.get("path") or "")
    source = str(change.get("source") or "unknown")
    before = _preview_display_value(change.get("before"))
    after = _preview_display_value(change.get("after"))
    return f"{path} [{source}]: {before} -> {after}"


def _preview_value_source_text(row: Mapping[str, JSONValue]) -> str:
    """Return one text row for a preview value source."""
    path = str(row.get("path") or "")
    source = str(row.get("source") or "unknown")
    value = _preview_display_value(row.get("value"))
    override = " override" if row.get("override") else ""
    previous = ""
    if row.get("override"):
        previous_source = str(row.get("previous_source") or "unknown")
        previous_value = _preview_display_value(row.get("previous_value"))
        previous = f"; previous={previous_source}:{previous_value}"
    return f"{path} [{source}{override}]: {value}{previous}"


def _preview_display_value(
    value: JSONValue | None,
    *,
    limit: int = QUALITY_PROFILE_PREVIEW_DISPLAY_VALUE_LIMIT,
) -> str:
    """Return a bounded deterministic display value."""
    rendered = json.dumps(value, sort_keys=True)
    if len(rendered) <= limit:
        return rendered
    return f"{rendered[: max(0, limit - 3)]}..."


def _markdown_cell(value: str) -> str:
    """Escape minimal Markdown table control characters."""
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def _preview_mapping(value: object) -> Mapping[str, JSONValue]:
    """Return a JSON mapping or an empty mapping."""
    if isinstance(value, Mapping):
        return cast(Mapping[str, JSONValue], value)
    return {}


def _preview_mapping_rows(value: object) -> list[Mapping[str, JSONValue]]:
    """Return mapping rows from a JSON array-like value."""
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [
            cast(Mapping[str, JSONValue], item)
            for item in value
            if isinstance(item, Mapping)
        ]
    return []


def _quality_profile_preview_payload(
    context: RuntimeContext,
    *,
    preview_format: str | None = None,
    preview_output_path: str | None = None,
) -> dict[str, JSONValue]:
    """Return a deterministic preview of the resolved quality profile."""
    profile = quality_profile_from_value(context.quality_profile)
    resolution = dict(context.quality_profile_resolution)
    if not resolution:
        resolution = quality_profile_resolution_from_value(profile).to_payload()
    resolved_profile = dict(
        _preview_mapping(resolution.get("resolved_profile"))
    )

    profile_metadata = profile.to_metadata()
    profile_metadata.setdefault("configured_reporting_keys", [])
    profile_metadata["configured_modeling_assumptions"] = dict(
        profile.modeling_assumptions
    )
    profile_metadata["reporting"] = profile.reporting_profile().to_metadata()

    audit_override_enabled = bool(
        context.args.get("quality_remediation_catalog_audit")
    )
    effective_sources = [
        dict(item)
        for item in _preview_mapping_rows(
            resolution.get("effective_value_sources")
        )
    ]
    cli_overrides = _quality_profile_cli_overrides(effective_sources)
    profile_inputs: dict[str, JSONValue] = {
        "from_api": context.from_api,
        "config_path": str(context.args.get("config_path") or ""),
        "quality_profile_path": context.quality_profile_path,
        "quality_profile_preview_format": (
            preview_format or context.quality_profile_preview_format
        ),
        "quality_profile_preview_output_path": (
            preview_output_path
            if preview_output_path is not None
            else context.quality_profile_preview_output_path
        ),
        "quality_remediation_catalog_audit": audit_override_enabled,
    }
    return {
        "schema_version": QUALITY_PROFILE_PREVIEW_SCHEMA_VERSION,
        "quality_modes": {
            "quality": context.data_quality,
            "repo_quality": context.repo_quality_refresh,
            "quality_preflight": context.quality_preflight,
        },
        "profile_inputs": profile_inputs,
        "cli_overrides": cli_overrides,
        "quality_profile": profile_metadata,
        "profile_explanation": _quality_profile_preview_explanation(
            profile,
            resolved_profile=resolved_profile,
            resolution=resolution,
        ),
        "resolved_profile": resolved_profile,
    }


def _quality_profile_preview_explanation(
    profile: QualityProfile,
    *,
    resolved_profile: Mapping[str, JSONValue],
    resolution: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    """Render provenance retained by the profile resolver."""
    default_profile = _resolved_default_quality_profile_payload()
    source_kind = _quality_profile_source_kind(profile)
    effective_sources = [
        dict(item)
        for item in _preview_mapping_rows(
            resolution.get("effective_value_sources")
        )
    ]
    effective_diff = _quality_profile_effective_diff(
        default_profile,
        resolved_profile,
        effective_sources=effective_sources,
    )
    return {
        "schema_version": QUALITY_PROFILE_PREVIEW_EXPLANATION_SCHEMA_VERSION,
        "profile_source": {
            "kind": source_kind,
            "profile_name": profile.name,
            "profile_source": profile.source,
            "source_path": profile.source_path,
            "is_default": profile.is_default,
        },
        "input_channels": cast(
            JSONValue,
            [
                dict(item)
                for item in _preview_mapping_rows(
                    resolution.get("input_channels")
                )
            ],
        ),
        "effective_value_sources": _bounded_source_items(
            effective_sources,
            limit=QUALITY_PROFILE_PREVIEW_SOURCE_LIMIT,
        ),
        "effective_diff": effective_diff,
    }


def _resolved_default_quality_profile_payload() -> dict[str, JSONValue]:
    """Return the normalized built-in default preview profile payload."""
    default_profile = quality_profile_from_value(None)
    payload: dict[str, JSONValue] = default_profile.to_request_payload()
    payload["reporting"] = default_profile.reporting_profile().to_metadata()
    return payload


def _quality_profile_cli_overrides(
    effective_sources: Sequence[Mapping[str, JSONValue]],
) -> dict[str, JSONValue]:
    """Return compatibility CLI overrides from resolver provenance."""
    overrides: dict[str, JSONValue] = {}
    for item in effective_sources:
        if item.get("source") != "cli_override" or not item.get("override"):
            continue
        path = str(item.get("path") or "")
        if not path:
            continue
        overrides[_dotted_path_from_json_pointer(path)] = item.get("value")
    return overrides


def _quality_profile_effective_diff(
    default_profile: Mapping[str, JSONValue],
    resolved_profile: Mapping[str, JSONValue],
    *,
    effective_sources: Sequence[Mapping[str, JSONValue]],
) -> dict[str, JSONValue]:
    """Return a bounded diff from the built-in default profile."""
    default_values = dict(_flatten_json_mapping(default_profile))
    resolved_values = dict(_flatten_json_mapping(resolved_profile))
    _prune_expanded_empty_mapping_values(default_values, resolved_values)
    _prune_expanded_empty_mapping_values(resolved_values, default_values)
    source_by_path = {
        str(item.get("path")): str(item.get("source") or "unknown")
        for item in effective_sources
    }
    changes: list[dict[str, JSONValue]] = []
    for path in sorted(set(default_values) | set(resolved_values)):
        before = default_values.get(path)
        after = resolved_values.get(path)
        if before == after:
            continue
        changes.append(
            {
                "path": path,
                "before": before,
                "after": after,
                "source": source_by_path.get(path, "unknown"),
            }
        )
    included = changes[:QUALITY_PROFILE_PREVIEW_DIFF_LIMIT]
    omitted_count = max(0, len(changes) - len(included))
    return {
        "schema_version": QUALITY_PROFILE_PREVIEW_DIFF_SCHEMA_VERSION,
        "base": "built_in_default",
        "changed_value_count": len(changes),
        "included_change_count": len(included),
        "omitted_change_count": omitted_count,
        "truncated": omitted_count > 0,
        "changes": cast(JSONValue, included),
    }


def _prune_expanded_empty_mapping_values(
    values: dict[str, JSONValue],
    other_values: Mapping[str, JSONValue],
) -> None:
    """Remove empty-object placeholders when the other side has child values."""
    for path, value in tuple(values.items()):
        if value != {}:
            continue
        child_prefix = f"{path}/"
        if any(
            other_path.startswith(child_prefix) for other_path in other_values
        ):
            del values[path]


def _quality_profile_source_kind(profile: QualityProfile) -> str:
    """Return a stable public source kind for a quality profile."""
    return quality_profile_source_kind(profile)


def _bounded_source_items(
    items: Sequence[dict[str, JSONValue]],
    *,
    limit: int,
) -> dict[str, JSONValue]:
    """Return bounded source items plus truncation metadata."""
    included = list(items[:limit])
    omitted_count = max(0, len(items) - len(included))
    return {
        "value_count": len(items),
        "included_value_count": len(included),
        "omitted_value_count": omitted_count,
        "truncated": omitted_count > 0,
        "values": cast(JSONValue, included),
    }


def _flatten_json_mapping(
    value: Mapping[str, JSONValue],
    *,
    prefix: str = "",
) -> list[tuple[str, JSONValue]]:
    """Return sorted JSON-pointer leaf values for a mapping."""
    flattened: list[tuple[str, JSONValue]] = []
    for key in sorted(value, key=str):
        path = f"{prefix}/{_json_pointer_token(str(key))}"
        item = value[key]
        if isinstance(item, Mapping):
            if item:
                flattened.extend(_flatten_json_mapping(item, prefix=path))
            else:
                flattened.append((path, {}))
        else:
            flattened.append((path, item))
    return flattened


def _dotted_path_from_json_pointer(path: str) -> str:
    """Translate a JSON pointer into the compatibility dotted path form."""
    return ".".join(
        _json_pointer_token_unescape(token)
        for token in path.strip("/").split("/")
        if token
    )


def _json_pointer_token(value: str) -> str:
    """Return a JSON Pointer token."""
    return value.replace("~", "~0").replace("/", "~1")


def _json_pointer_token_unescape(value: str) -> str:
    """Return an unescaped JSON Pointer token."""
    return value.replace("~1", "/").replace("~0", "~")


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
            f"influxdb.yaml is missing required influxdb keys: {missing_text}."
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
            f"influx metadata is missing required keys: {missing_text}."
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
        {"analytics", "cleanup", "groups", "jobs", "quality", "runtime"},
    )
    if not options and routed_command == "cleanup":
        from histdatacom.cleanup_cli import main as cleanup_main

        return cleanup_main(
            remove_routed_command_from_cli_args(cli_args, "cleanup")
        )
    if not options and routed_command == "jobs":
        from histdatacom.orchestration.cli import jobs_main

        return jobs_main(remove_routed_command_from_cli_args(cli_args, "jobs"))
    if not options and routed_command == "groups":
        from histdatacom.groups_cli import main as groups_main

        return groups_main(
            remove_routed_command_from_cli_args(cli_args, "groups")
        )
    if not options and routed_command == "quality":
        from histdatacom.quality_cli import main as quality_main

        return quality_main(
            remove_routed_command_from_cli_args(cli_args, "quality")
        )
    if not options and routed_command == "runtime":
        reexec_code = _maybe_reexec_windows_runtime_cli(cli_args)
        if reexec_code is not None:
            return reexec_code

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


def _maybe_reexec_windows_runtime_cli(
    cli_args: Sequence[str],
) -> int | None:
    """Route Windows runtime management away from console launcher parents."""
    if os.name != "nt":
        return None
    if os.environ.get(WINDOWS_RUNTIME_REEXEC_ENV) == "1":
        return None
    python = _windows_runtime_reexec_python()
    if python is None:
        return None

    env = dict(os.environ)
    env[WINDOWS_RUNTIME_REEXEC_ENV] = "1"
    completed = subprocess.run(
        [python, "-m", "histdatacom", *cli_args],
        check=False,
        env=env,
        creationflags=_windows_runtime_reexec_creationflags(),
    )
    return int(completed.returncode)


def _windows_runtime_reexec_creationflags() -> int:
    """Return Windows process-group flags for the runtime CLI trampoline."""
    if os.name != "nt":
        return 0
    return int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))


def _windows_runtime_reexec_python() -> str | None:
    """Return the Python executable for Windows runtime CLI re-exec."""
    executable = str(sys.executable)
    executable_name = os.path.basename(executable).lower()
    if executable_name in WINDOWS_PYTHON_EXECUTABLE_NAMES:
        argv0_name = os.path.basename(str(sys.argv[0])).lower()
        if argv0_name in WINDOWS_HISTDATACOM_LAUNCHER_NAMES:
            return executable
        return None
    return _windows_python_for_launcher(executable)


def _windows_python_for_launcher(launcher: str) -> str | None:
    """Resolve the Python executable beside a Windows console launcher."""
    launcher_dir = os.path.dirname(launcher)
    candidates = (
        os.path.join(launcher_dir, "python.exe"),
        os.path.join(os.path.dirname(launcher_dir), "python.exe"),
        str(getattr(sys, "_base_executable", "")),
    )
    for candidate in candidates:
        if os.path.basename(
            candidate
        ).lower() in WINDOWS_PYTHON_EXECUTABLE_NAMES and os.path.exists(
            candidate
        ):
            return candidate
    return None


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
            f"quality report cleanup: {report_disposition['delete_error']}"
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
            f"{source_state} ({source_count} transient ZIP/CSV)"
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
    lines.extend(
        format_quality_engine_skip_lines(
            _mapping_from_payload(quality_payload.get("quality_engine"))
        )
    )
    lines.extend(
        format_quality_next_action_lines(
            _mapping_from_payload(quality_payload.get("next_actions"))
        )
    )
    lines.extend(
        format_quality_remediation_coverage_lines(
            _mapping_from_payload(quality_payload.get("remediation_coverage"))
        )
    )
    lines.extend(
        format_fingerprint_distribution_attention_lines(
            _mapping_from_payload(
                quality_payload.get("fingerprint_distribution_attention")
            )
        )
    )
    lines.extend(
        format_fingerprint_distribution_summary_lines(
            _mapping_from_payload(
                quality_payload.get("fingerprint_distribution")
            )
        )
    )
    lines.extend(
        format_fingerprint_topology_attention_lines(
            _mapping_from_payload(
                quality_payload.get("fingerprint_topology_attention")
            )
        )
    )
    lines.extend(
        format_fingerprint_topology_summary_lines(
            _mapping_from_payload(quality_payload.get("fingerprint_topology"))
        )
    )
    lines.extend(
        format_fingerprint_readiness_risk_lines(
            _mapping_from_payload(
                quality_payload.get("fingerprint_readiness_risk")
            )
        )
    )
    lines.extend(
        format_cross_series_fingerprint_lines(
            _mapping_from_payload(
                quality_payload.get("fingerprint_cross_series")
            )
        )
    )
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
        return f"targets: {summary.get('target_count', 0)}"
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

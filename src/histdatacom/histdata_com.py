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
from typing import TYPE_CHECKING, Any, Mapping

import histdatacom
from histdatacom import Options
from histdatacom.cli import ArgParser
from histdatacom.data_quality import (
    QualityDiscoveryError,
    discover_quality_targets,
    normalize_quality_check_groups,
    run_quality_assessment,
)
from histdatacom.exceptions import InfluxConfigurationError
from histdatacom.repository_output import (
    print_repository_failure,
    print_repository_table,
)
from histdatacom.histdata_ascii import CACHE_FILENAME
from histdatacom.records import Record
from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar.client import (
    SidecarUnavailableError,
    submit_run_request_and_observe_sync,
)
from histdatacom.sidecar.cutover import (
    FOREGROUND_RUNTIME_REMOVED_MESSAGE,
    should_submit_to_sidecar,
)
from histdatacom.utils import (
    load_influx_yaml,
    set_working_data_dir,
    normalize_api_return_type,
)

if TYPE_CHECKING:
    from pandas import DataFrame
    from polars import DataFrame as PolarsDataFrame
    from pyarrow import Table


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    """Resolved launch context for sidecar execution."""

    args: Mapping[str, Any]
    request: RunRequest
    version: bool
    from_api: bool
    sidecar_start: bool
    sidecar_wait_result: bool
    api_return_type: str | None
    data_quality: bool
    quality_paths: tuple[str, ...]
    quality_check_groups: tuple[str, ...]
    available_remote_data: bool
    update_remote_data: bool
    import_to_influxdb: bool


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
              - vars(...): get the __dict__ representation of the object
              - ArgParser._arg_list_to_set(...)
                  - Normalize iterable user arguments whose values are lists and
                    make them sets instead
          - .copy(): decouple for GC using a hard copy of user args
        """
        self.options = ArgParser(options)()
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

        if self.context.data_quality:
            return self._run_data_quality()

        return self._run_sidecar_job()

    def _run_data_quality(self) -> dict:
        """Run local-only data-quality target discovery."""
        try:
            check_groups = normalize_quality_check_groups(
                self.context.quality_check_groups
            )
            discovery = discover_quality_targets(self.context.quality_paths)
        except QualityDiscoveryError as err:
            if self.context.from_api:
                raise
            print(f"error: {err}", file=sys.stderr)  # noqa:T201
            raise SystemExit(1) from err

        report = run_quality_assessment(
            discovery.targets,
            (),
            metadata={
                "operation": "data-quality",
                "check_groups": list(check_groups),
            },
        )
        payload = {
            "operation": "data-quality",
            "check_groups": list(check_groups),
            "discovery": discovery.to_dict(),
            "summary": report.summary().to_dict(),
        }
        if not self.context.from_api:
            _print_data_quality_discovery(payload)
        return payload

    def _run_sidecar_job(
        self,
    ) -> list | dict | PolarsDataFrame | DataFrame | Table:
        """Submit this run to the Temporal sidecar client boundary."""
        try:
            result = submit_run_request_and_observe_sync(
                self.context.request,
                start_if_needed=self.context.sidecar_start,
                wait_for_result=self.context.sidecar_wait_result,
            )
        except SidecarUnavailableError as err:
            if self.context.from_api:
                raise
            print(f"error: {err}", file=sys.stderr)  # noqa:T201
            raise SystemExit(1) from err

        payload = result.to_dict()
        if self._should_materialize_sidecar_repository_return():
            if _sidecar_repository_payload_failed(payload):
                if self.context.from_api:
                    return (
                        _repository_available_data_from_sidecar_payload(payload)
                        or {}
                    )
                print_repository_failure(
                    _repository_failure_code_from_sidecar_payload(payload)
                )
                raise SystemExit(1)

            available_data = _repository_available_data_from_sidecar_payload(
                payload
            )
            if available_data is not None:
                if self.context.from_api:
                    return available_data
                print_repository_table(available_data)
                raise SystemExit(0)

        if self.context.sidecar_wait_result and _sidecar_payload_failed(
            payload
        ):
            if not self.context.from_api:
                print(
                    json.dumps(payload, indent=2, sort_keys=True)
                )  # noqa:T201
                _print_sidecar_payload_failure(payload)
                raise SystemExit(1)
            return payload

        if self._should_materialize_sidecar_api_return(payload):
            records = _cache_records_from_sidecar_payload(payload)
            if records:
                return self._materialize_sidecar_api_return(records)
        if not self.context.from_api:
            print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return payload

    def _should_materialize_sidecar_repository_return(self) -> bool:
        """Return whether a waited sidecar repo request should mimic legacy IO."""
        return bool(
            self.context.sidecar_wait_result
            and (
                self.context.available_remote_data
                or self.context.update_remote_data
            )
        )

    def _should_materialize_sidecar_api_return(self, payload: dict) -> bool:
        """Return whether a completed sidecar run should mimic API returns."""
        return bool(
            self.context.from_api
            and self.context.api_return_type
            and self.context.sidecar_wait_result
            and payload.get("status") == "completed"
            and payload.get("result")
        )

    def _materialize_sidecar_api_return(
        self,
        records: list[Record],
    ) -> list | PolarsDataFrame | DataFrame | Table:
        """Rebuild the legacy API dataframe return from sidecar cache artifacts."""
        from histdatacom.api import Api

        return Api().merge_records(
            records,
            return_type=str(self.context.api_return_type or ""),
        )


def _resolve_runtime_context(options: Options) -> RuntimeContext:
    """Resolve launch values without touching process-global config."""
    args = ArgParser.arg_list_to_set(vars(options)).copy()
    args["default_download_dir"] = set_working_data_dir(args["data_directory"])
    args["api_return_type"] = normalize_api_return_type(args["api_return_type"])
    options.api_return_type = args["api_return_type"]
    _attach_influx_config_metadata(options, args)
    try:
        should_submit_to_sidecar(args)
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
        sidecar_start=bool(args["sidecar_start"]),
        sidecar_wait_result=bool(args["sidecar_wait_result"]),
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
        available_remote_data=bool(args["available_remote_data"]),
        update_remote_data=bool(args["update_remote_data"]),
        import_to_influxdb=bool(args["import_to_influxdb"]),
    )


def _attach_influx_config_metadata(
    options: Options,
    args: dict[str, Any],
) -> None:
    """Snapshot caller-local Influx config before sidecar handoff."""
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
    """Validate serialized sidecar Influx connection keys."""
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


def _print_data_quality_discovery(payload: Mapping[str, Any]) -> None:
    """Print a compact local target discovery summary."""
    discovery = payload.get("discovery", {})
    if not isinstance(discovery, Mapping):
        discovery = {}
    targets = discovery.get("targets", [])
    if not isinstance(targets, list):
        targets = []

    print("Data quality target discovery")  # noqa:T201
    print(  # noqa:T201
        "checks: " + ", ".join(str(item) for item in payload["check_groups"])
    )
    print(f"roots: {len(discovery.get('roots', []))}")  # noqa:T201
    print(f"targets: {len(targets)}")  # noqa:T201
    if not targets:
        print("No data quality targets discovered.")  # noqa:T201
        return
    for target in targets:
        if not isinstance(target, Mapping):
            continue
        print(  # noqa:T201
            f"- {target.get('kind', 'unknown')}: {target.get('path', '')}"
        )


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
    if not options and len(sys.argv) > 1 and sys.argv[1] == "sidecar":
        from histdatacom.sidecar.cli import main as sidecar_main

        return sidecar_main(sys.argv[2:])

    if not options:
        options = Options()
        _HistDataCom(options).run()
        return None
    options.from_api = True
    return _HistDataCom(options).run()


def _cache_records_from_sidecar_payload(payload: dict) -> list[Record]:
    """Return legacy records reconstructed from sidecar cache artifacts."""
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


def _repository_available_data_from_sidecar_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return legacy repository data from sidecar result metrics."""
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


def _sidecar_repository_payload_failed(payload: Mapping[str, Any]) -> bool:
    """Return whether the waited sidecar result represents repo failure."""
    result = payload.get("result")
    if isinstance(result, Mapping):
        status = str(result.get("status", "") or "").lower()
        if status in {"failed", "cancelled"}:
            return True
    return bool(_repository_failure_code_from_sidecar_payload(payload))


def _sidecar_payload_failed(payload: Mapping[str, Any]) -> bool:
    """Return whether a waited sidecar result represents failed work."""
    return _sidecar_failure_status(payload) in {
        WorkStatus.FAILED,
        WorkStatus.CANCELLED,
    }


def _sidecar_failure_status(
    payload: Mapping[str, Any],
) -> WorkStatus | None:
    """Return the terminal failure status from a sidecar payload."""
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


def _print_sidecar_payload_failure(payload: Mapping[str, Any]) -> None:
    """Print a concise CLI error for failed waited sidecar jobs."""
    status = _sidecar_failure_status(payload) or WorkStatus.FAILED
    message = _sidecar_failure_message(payload)
    suffix = f": {message}" if message else ""
    print(
        f"error: sidecar job {status.value.lower()}{suffix}",
        file=sys.stderr,
    )  # noqa:T201


def _sidecar_failure_message(payload: Mapping[str, Any]) -> str:
    """Return the first useful failure message from a sidecar payload."""
    for item in _iter_mapping_payloads(payload):
        failure = item.get("failure")
        if isinstance(failure, Mapping) and failure.get("message"):
            return str(failure.get("message"))
        last_error = item.get("last_error")
        if last_error:
            return str(last_error)
    return ""


def _repository_failure_code_from_sidecar_payload(
    payload: Mapping[str, Any],
) -> str:
    """Return the first structured failure code in a sidecar result payload."""
    for item in _iter_mapping_payloads(payload):
        failure = item.get("failure")
        if not isinstance(failure, Mapping):
            continue
        code = failure.get("code")
        if code:
            return str(code)
    return ""


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
    """Collect dictionaries from nested sidecar result payloads."""
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
    """Collect artifact dictionaries from nested sidecar result payloads."""
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

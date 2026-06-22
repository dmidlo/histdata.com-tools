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

import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

import histdatacom
from histdatacom import Options, config
from histdatacom.cli import ArgParser
from histdatacom.foreground import (
    print_repository_failure,
    print_repository_table,
    run_foreground,
)
from histdatacom.histdata_ascii import CACHE_FILENAME
from histdatacom.records import Record
from histdatacom.runtime_contracts import RunRequest
from histdatacom.sidecar.client import (
    SidecarUnavailableError,
    submit_run_request_and_observe_sync,
)
from histdatacom.sidecar.cutover import should_submit_to_sidecar
from histdatacom.utils import (
    load_influx_yaml,
    set_working_data_dir,
    check_installed_module,
    normalize_api_return_type,
)

if TYPE_CHECKING:
    from pandas import DataFrame
    from polars import DataFrame as PolarsDataFrame
    from pyarrow import Table


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
        config.ARGS = ArgParser.arg_list_to_set(  # noqa:BLK100
            vars(self.options)  # noqa:WPS110
        ).copy()
        config.ARGS["default_download_dir"] = set_working_data_dir(
            config.ARGS["data_directory"]
        )
        config.ARGS["api_return_type"] = normalize_api_return_type(
            config.ARGS["api_return_type"]
        )
        self.options.api_return_type = config.ARGS["api_return_type"]

        if config.ARGS["version"] or self._uses_sidecar():
            return

        if config.ARGS["import_to_influxdb"]:
            check_installed_module("influxdb_client")
            influx_yaml = load_influx_yaml()
            config.ARGS["INFLUX_ORG"] = influx_yaml["influxdb"]["org"]
            config.ARGS["INFLUX_BUCKET"] = influx_yaml["influxdb"]["bucket"]
            config.ARGS["INFLUX_URL"] = influx_yaml["influxdb"]["url"]
            config.ARGS["INFLUX_TOKEN"] = influx_yaml["influxdb"]["token"]

        if (
            config.ARGS["from_api"]
            and config.ARGS["api_return_type"]
            and not config.ARGS["version"]
            and not config.ARGS["available_remote_data"]
            and not config.ARGS["update_remote_data"]
        ):
            check_installed_module(config.ARGS["api_return_type"])

        if config.ARGS["import_to_influxdb"]:
            config.ARGS["api_return_type"] = "polars"
            check_installed_module(config.ARGS["api_return_type"])

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
        if config.ARGS["version"]:
            if not config.ARGS["from_api"]:
                print(histdatacom.__version__)  # noqa:T201
            return histdatacom.__version__

        if self._uses_sidecar():
            return self._run_sidecar_job()

        return run_foreground(
            RunRequest.from_options(self.options), config.ARGS
        )

    def _uses_sidecar(self) -> bool:
        """Return whether this foreground run should submit to the sidecar."""
        return should_submit_to_sidecar(config.ARGS)

    def _run_sidecar_job(
        self,
    ) -> list | dict | PolarsDataFrame | DataFrame | Table:
        """Submit this run to the Temporal sidecar client boundary."""
        request = RunRequest.from_options(self.options)
        try:
            result = submit_run_request_and_observe_sync(
                request,
                start_if_needed=bool(config.ARGS["sidecar_start"]),
                wait_for_result=bool(config.ARGS["sidecar_wait_result"]),
            )
        except SidecarUnavailableError as err:
            if config.ARGS["from_api"]:
                raise
            print(f"error: {err}", file=sys.stderr)  # noqa:T201
            raise SystemExit(1) from err

        payload = result.to_dict()
        if self._should_materialize_sidecar_repository_return():
            if _sidecar_repository_payload_failed(payload):
                if config.ARGS["from_api"]:
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
                if config.ARGS["from_api"]:
                    return available_data
                print_repository_table(available_data)
                raise SystemExit(0)

        if self._should_materialize_sidecar_api_return(payload):
            records = _cache_records_from_sidecar_payload(payload)
            if records:
                return self._materialize_sidecar_api_return(records)
        if not config.ARGS["from_api"]:
            print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return payload

    def _should_materialize_sidecar_repository_return(self) -> bool:
        """Return whether a waited sidecar repo request should mimic legacy IO."""
        return bool(
            config.ARGS["sidecar_wait_result"]
            and (
                config.ARGS["available_remote_data"]
                or config.ARGS["update_remote_data"]
            )
        )

    def _should_materialize_sidecar_api_return(self, payload: dict) -> bool:
        """Return whether a completed sidecar run should mimic API returns."""
        return bool(
            config.ARGS["from_api"]
            and config.ARGS["api_return_type"]
            and config.ARGS["sidecar_wait_result"]
            and payload.get("status") == "completed"
            and payload.get("result")
        )

    def _materialize_sidecar_api_return(
        self,
        records: list[Record],
    ) -> list | PolarsDataFrame | DataFrame | Table:
        """Rebuild the legacy API dataframe return from sidecar cache artifacts."""
        from histdatacom.api import Api

        return Api().merge_records(records)


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
        status="CACHE_READY",
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
